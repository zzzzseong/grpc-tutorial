// trigger — 데모를 시작시키기 위한 아주 작은 CLI일 뿐입니다.
// 아키텍처의 일부가 아니라, order-server에 주문 생성을 한 번 던져
// order ↔ payment 상호 호출 흐름을 켜는 "발사 버튼" 역할입니다.
//
// (실무라면 이 호출도 또 다른 서버나 API 게이트웨이 안에 들어갑니다.)
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

	// `go run ./trigger`        → 1단계 unary 버전
	// `go run ./trigger stream` → 2단계 server streaming 버전
	if len(os.Args) > 1 && os.Args[1] == "stream" {
		runStream(ctx, client)
		return
	}
	runUnary(ctx, client)
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
