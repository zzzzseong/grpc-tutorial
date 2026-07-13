// payment-server — 결제 서버

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

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

// ProcessPayment order-server가 결제를 요청하면 실행 (payment-server가 server 역할).
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

// ProcessPaymentStream [2단계: Server Streaming] 결제 진행 상황을 스트림으로 흘려보냅니다.
//
// Unary와 시그니처가 다른 점 두 가지:
//  1. 응답을 return하지 않습니다. 대신 stream.Send()를 원하는 만큼 호출합니다.
//  2. ctx 파라미터가 없습니다. 필요하면 stream.Context()로 꺼냅니다.
//
// 함수가 return하는 순간 gRPC가 "스트림 끝"을 클라이언트에 알립니다.
// (클라이언트 쪽에서는 Recv()가 io.EOF를 받는 순간)
func (s *paymentServer) ProcessPaymentStream(req *paymentpb.ProcessPaymentRequest, stream paymentpb.PaymentService_ProcessPaymentStreamServer) error {
	log.Printf("[payment] (stream) 결제 요청 수신: %s, %d원", req.GetOrderId(), req.GetAmount())

	// 결제가 여러 단계를 거친다고 가정하고, 단계마다 진행 상황을 Send.
	// time.Sleep은 외부 결제사 API 지연을 흉내 낸 것입니다.
	for _, stage := range []string{"VALIDATING", "AUTHORIZING"} {
		if err := stream.Send(&paymentpb.PaymentProgress{Stage: stage}); err != nil {
			return err // 클라이언트가 연결을 끊었거나 데드라인 초과
		}
		log.Printf("[payment] (stream)   → %s 전송", stage)
		time.Sleep(700 * time.Millisecond)
	}

	s.mu.Lock()
	s.seq++
	txID := fmt.Sprintf("TX-%03d", s.seq)
	s.mu.Unlock()

	// 결제 완료 → 기존 unary와 동일하게 order-server의 상태를 갱신 (여기선 client 역할)
	if _, err := s.order.UpdateOrderStatus(stream.Context(), &orderpb.UpdateOrderStatusRequest{
		OrderId: req.GetOrderId(),
		Status:  "PAID",
	}); err != nil {
		log.Printf("[payment] (stream) 상태 갱신 호출 실패: %v", err)
	}

	// 마지막 조각: 승인 완료 + 거래 ID
	if err := stream.Send(&paymentpb.PaymentProgress{Stage: "APPROVED", TransactionId: txID}); err != nil {
		return err
	}
	log.Printf("[payment] (stream) 결제 승인 완료: %s (%s)", req.GetOrderId(), txID)

	return nil // ← 여기서 스트림이 정상 종료됩니다
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
