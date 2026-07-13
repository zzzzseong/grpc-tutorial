// payment-server — 결제 서버.
//
// order-server와 대칭 구조입니다:
//   - server 역할: PaymentService를 구현하고 :50052 포트를 엽니다.
//   - client 역할: order-server의 OrderService 스텁을 들고 상태 갱신을 호출합니다.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orderpb "grpc-tutorial/gen/orderpb"
	paymentpb "grpc-tutorial/gen/paymentpb"
)

type paymentServer struct {
	paymentpb.UnimplementedPaymentServiceServer

	// ↓ order-server를 호출하기 위한 클라이언트 스텁 (client 역할)
	order orderpb.OrderServiceClient

	mu  sync.Mutex
	seq int
}

// ProcessPayment: order-server가 결제를 요청하면 실행 (payment-server가 server 역할).
func (s *paymentServer) ProcessPayment(ctx context.Context, req *paymentpb.ProcessPaymentRequest) (*paymentpb.ProcessPaymentReply, error) {
	log.Printf("[payment] 결제 요청 수신: %s, %d원", req.GetOrderId(), req.GetAmount())

	// 1. (가짜) 결제 처리
	s.mu.Lock()
	s.seq++
	txID := fmt.Sprintf("TX-%03d", s.seq)
	s.mu.Unlock()

	// 2. 결제 완료 → order-server를 호출해 주문 상태를 PAID로 갱신.
	//    여기서 payment-server는 client가 됩니다.
	if _, err := s.order.UpdateOrderStatus(ctx, &orderpb.UpdateOrderStatusRequest{
		OrderId: req.GetOrderId(),
		Status:  "PAID",
	}); err != nil {
		log.Printf("[payment] 상태 갱신 호출 실패: %v", err)
	}

	log.Printf("[payment] 결제 승인 완료: %s (%s)", req.GetOrderId(), txID)
	return &paymentpb.ProcessPaymentReply{Approved: true, TransactionId: txID}, nil
}

func main() {
	// order-server를 호출할 클라이언트 준비 (lazy 연결)
	orderConn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[payment] order 연결 준비 실패: %v", err)
	}
	defer orderConn.Close()

	srv := &paymentServer{
		order: orderpb.NewOrderServiceClient(orderConn),
	}

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("[payment] 포트 열기 실패: %v", err)
	}

	g := grpc.NewServer()
	paymentpb.RegisterPaymentServiceServer(g, srv)
	log.Println("[payment] gRPC 서버 시작 :50052")
	if err := g.Serve(lis); err != nil {
		log.Fatalf("[payment] 서버 실행 실패: %v", err)
	}
}
