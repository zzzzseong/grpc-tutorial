// trigger — 데모를 시작시키기 위한 아주 작은 CLI일 뿐입니다.
// 아키텍처의 일부가 아니라, order-server에 주문 생성을 한 번 던져
// order ↔ payment 상호 호출 흐름을 켜는 "발사 버튼" 역할입니다.
//
// (실무라면 이 호출도 또 다른 서버나 API 게이트웨이 안에 들어갑니다.)
package main

import (
	"context"
	"log"
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	res, err := client.CreateOrder(ctx, &orderpb.CreateOrderRequest{
		Item:   "커피",
		Amount: 4500,
	})
	if err != nil {
		log.Fatalf("주문 실패: %v", err)
	}

	log.Printf("[trigger] 주문 결과: id=%s, status=%s", res.GetOrderId(), res.GetStatus())
}
