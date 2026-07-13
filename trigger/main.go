// trigger — 데모 시작 트리거

package main

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orderpb "grpc-tutorial/gen/orderpb"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("연결 실패: %v", err)
	}
	defer conn.Close()

	client := orderpb.NewOrderServiceClient(conn)

	// 스트리밍은 중간 응답이 계속 오므로 타임아웃을 넉넉히 잡습니다.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// `go run ./trigger`         → 1단계 unary 버전
	// `go run ./trigger stream`  → 2단계 server streaming 버전
	// `go run ./trigger bulk`    → 3단계 client streaming 버전
	// `go run ./trigger session` → 4단계 bidirectional streaming 버전
	mode := ""
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	switch mode {
	case "stream":
		runStream(ctx, client)
	case "bulk":
		runBulk(ctx, client)
	case "session":
		runSession(ctx, client)
	default:
		runUnary(ctx, client)
	}
}

// runUnary [1단계] 요청 1개 → 응답 1개. 결과는 다 끝난 뒤 한 번에 옵니다.
func runUnary(ctx context.Context, client orderpb.OrderServiceClient) {
	res, err := client.CreateOrder(ctx, &orderpb.CreateOrderRequest{
		Item:   "커피",
		Amount: 4500,
	})
	if err != nil {
		log.Fatalf("주문 실패: %v", err)
	}
	log.Printf("[trigger] 주문 결과: id=%s, status=%s", res.GetOrderId(), res.GetStatus())
}

// runStream [2단계] 요청 1개 → 응답 여러 개. 진행 상황이 실시간으로 흘러옵니다.
func runStream(ctx context.Context, client orderpb.OrderServiceClient) {
	stream, err := client.CreateOrderWithProgress(ctx, &orderpb.CreateOrderRequest{
		Item:   "커피",
		Amount: 4500,
	})
	if err != nil {
		log.Fatalf("주문 실패: %v", err)
	}

	// 스트림 수신 관용구: io.EOF가 올 때까지 Recv() 반복
	for {
		prog, err := stream.Recv()
		if err == io.EOF {
			break // 서버가 스트림을 정상 종료
		}
		if err != nil {
			log.Fatalf("수신 실패: %v", err)
		}
		log.Printf("[trigger] %s | %-20s | %s", prog.GetOrderId(), prog.GetStage(), prog.GetMessage())
	}
	log.Println("[trigger] 스트림 종료")
}

// runBulk [3단계] 요청 여러 개 → 응답 1개. 장바구니에 하나씩 담아 보냅니다.
func runBulk(ctx context.Context, client orderpb.OrderServiceClient) {
	// 호출 시점엔 아직 아무것도 안 보냅니다. "송신용 스트림"만 열립니다.
	stream, err := client.CreateBulkOrder(ctx)
	if err != nil {
		log.Fatalf("스트림 열기 실패: %v", err)
	}

	cart := []struct {
		item   string
		amount int32
	}{
		{"커피", 4500},
		{"샌드위치", 6800},
		{"쿠키", 2500},
	}

	// 상품을 하나씩 Send. (사용자가 장바구니에 담는 간격을 흉내)
	for _, c := range cart {
		if err := stream.Send(&orderpb.CartItem{Item: c.item, Amount: c.amount}); err != nil {
			log.Fatalf("전송 실패: %v", err)
		}
		log.Printf("[trigger] 장바구니에 담음: %s (%d원)", c.item, c.amount)
		time.Sleep(500 * time.Millisecond)
	}

	// CloseAndRecv: "다 보냈다"고 알리고(서버 Recv()에 io.EOF 발생),
	// 서버의 응답 1개를 기다립니다. (client streaming 전용 메서드)
	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("주문 실패: %v", err)
	}
	log.Printf("[trigger] 주문 결과: id=%s, status=%s, 상품 %d개, 합계 %d원",
		res.GetOrderId(), res.GetStatus(), res.GetItemCount(), res.GetTotalAmount())
}

// runSession [4단계] 양방향 스트림. 송신과 수신이 동시에 일어납니다.
//
// 그래서 goroutine이 필요합니다: 수신 루프를 별도 goroutine에서 돌리고,
// 메인은 그동안 주문을 계속 Send합니다. 서로 기다리지 않는 전이중 통신이라
// 주문을 다 보내기 전에도 먼저 끝난 결과가 도착할 수 있습니다.
func runSession(ctx context.Context, client orderpb.OrderServiceClient) {
	stream, err := client.OrderSession(ctx)
	if err != nil {
		log.Fatalf("세션 열기 실패: %v", err)
	}

	// 수신 goroutine: 서버가 스트림을 닫을 때까지(io.EOF) 결과를 받는다
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				return // 서버가 세션을 정상 종료
			}
			if err != nil {
				log.Fatalf("수신 실패: %v", err)
			}
			log.Printf("[trigger] ← 결과 도착: %s (%s) %s", res.GetOrderId(), res.GetItem(), res.GetStatus())
		}
	}()

	// 송신: 수신과 상관없이 주문을 계속 흘려보낸다
	orders := []struct {
		item   string
		amount int32
	}{
		{"노트북", 9000}, // 금액이 클수록 처리(심사)가 오래 걸리게 해뒀음
		{"커피", 4500},   // → 나중에 보낸 주문의 결과가 먼저 도착하는 걸 관찰
		{"쿠키", 1500},
	}
	for _, o := range orders {
		if err := stream.Send(&orderpb.CreateOrderRequest{Item: o.item, Amount: o.amount}); err != nil {
			log.Fatalf("전송 실패: %v", err)
		}
		log.Printf("[trigger] → 주문 전송: %s (%d원)", o.item, o.amount)
		time.Sleep(300 * time.Millisecond)
	}

	// CloseSend: "더 보낼 요청 없음"만 알립니다. 수신은 계속됩니다!
	// (3단계의 CloseAndRecv와 달리 응답을 기다리지 않음)
	if err := stream.CloseSend(); err != nil {
		log.Fatalf("송신 종료 실패: %v", err)
	}
	log.Println("[trigger] 송신 종료. 남은 결과 수신 대기...")

	<-done // 수신 goroutine이 io.EOF를 만날 때까지 대기
	log.Println("[trigger] 세션 종료")
}
