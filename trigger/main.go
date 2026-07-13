// trigger — 데모 시작 트리거

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

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

	// [5단계: 메타데이터] 요청 추적 ID를 실어 보냅니다. HTTP 헤더 같은 존재라
	// 어떤 rpc를 호출하든 함께 전달되고, 서버의 인터셉터가 이걸 읽어 로깅합니다.
	reqID := fmt.Sprintf("req-%d", os.Getpid())
	ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", reqID)
	log.Printf("[trigger] x-request-id=%s", reqID)

	// `go run ./trigger`          → 1단계 unary 버전
	// `go run ./trigger stream`   → 2단계 server streaming 버전
	// `go run ./trigger bulk`     → 3단계 client streaming 버전
	// `go run ./trigger session`  → 4단계 bidirectional streaming 버전
	// `go run ./trigger fail`     → 5단계 에러 처리 (결제 한도 초과)
	// `go run ./trigger deadline` → 5단계 데드라인 전파 (1초 제한)
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
	case "fail":
		runFail(ctx, client)
	case "deadline":
		runDeadline(client)
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

// runFail [5단계: 에러 처리] 결제 한도(50,000원)를 넘는 주문을 일부러 보냅니다.
//
// payment-server가 만든 FailedPrecondition 에러가
// order-server를 거쳐 여기까지 코드 그대로 전파되는 걸 확인합니다.
func runFail(ctx context.Context, client orderpb.OrderServiceClient) {
	res, err := client.CreateOrder(ctx, &orderpb.CreateOrderRequest{
		Item:   "노트북",
		Amount: 60000, // 한도 초과!
	})
	if err != nil {
		// status.Convert: error에서 gRPC 상태(코드 + 메시지)를 꺼냅니다.
		st := status.Convert(err)
		log.Printf("[trigger] 주문 실패 — code=%s", st.Code())
		log.Printf("[trigger]          message=%s", st.Message())
		return
	}
	log.Printf("[trigger] 주문 결과: id=%s, status=%s", res.GetOrderId(), res.GetStatus())
}

// runDeadline [5단계: 데드라인 전파] 1초짜리 빡빡한 데드라인으로 스트리밍을 호출합니다.
//
// payment-server의 결제는 1.4초 이상 걸리므로 중간에 데드라인이 터집니다.
// 포인트: 데드라인은 ctx를 타고 trigger → order → payment까지 전파되어,
// 두 홉 건너에 있는 payment-server의 작업까지 함께 중단됩니다.
func runDeadline(client orderpb.OrderServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	stream, err := client.CreateOrderWithProgress(ctx, &orderpb.CreateOrderRequest{
		Item:   "커피",
		Amount: 4500,
	})
	if err != nil {
		log.Fatalf("주문 실패: %v", err)
	}

	for {
		prog, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			st := status.Convert(err)
			log.Printf("[trigger] 스트림 중단 — code=%s, message=%s", st.Code(), st.Message())
			return
		}
		log.Printf("[trigger] %s | %-20s | %s", prog.GetOrderId(), prog.GetStage(), prog.GetMessage())
	}
	log.Println("[trigger] 스트림 종료 (데드라인 안에 완료됨)")
}
