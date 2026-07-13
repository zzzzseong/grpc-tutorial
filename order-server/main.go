// order-server — 주문 서버.
//
// 이 프로세스 하나가 두 가지 역할을 동시에 합니다:
//   - server 역할: OrderService를 구현하고 :50051 포트를 엽니다.
//   - client 역할: payment-server의 PaymentService 스텁을 들고 결제를 호출합니다.
// 즉 "gRPC 서버 = 남을 못 부른다"가 아니라, 한 서버가 서버이자 클라이언트입니다.
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

type orderServer struct {
	orderpb.UnimplementedOrderServiceServer

	// ↓ payment-server를 호출하기 위한 클라이언트 스텁 (client 역할)
	payment paymentpb.PaymentServiceClient

	mu     sync.Mutex        // orders 맵을 여러 goroutine이 동시에 건드리므로 보호
	orders map[string]string // orderID -> status
	seq    int
}

// CreateOrder: 외부에서 주문 생성을 요청하면 실행 (order-server가 server 역할).
func (s *orderServer) CreateOrder(ctx context.Context, req *orderpb.CreateOrderRequest) (*orderpb.CreateOrderReply, error) {
	// 1. 주문을 만들어 CREATED 상태로 저장
	s.mu.Lock()
	s.seq++
	orderID := fmt.Sprintf("ORD-%03d", s.seq)
	s.orders[orderID] = "CREATED"
	s.mu.Unlock()
	log.Printf("[order] 주문 생성 %s (%s, %d원) → payment-server에 결제 요청", orderID, req.GetItem(), req.GetAmount())

	// 2. payment-server 호출! 여기서 order-server는 client가 됩니다.
	//    ctx를 그대로 넘기면 호출자의 데드라인이 payment-server까지 전파됩니다.
	payRes, err := s.payment.ProcessPayment(ctx, &paymentpb.ProcessPaymentRequest{
		OrderId: orderID,
		Amount:  req.GetAmount(),
	})
	if err != nil {
		return nil, err
	}

	// 3. 최종 상태를 읽어 응답.
	//    (아래 UpdateOrderStatus가 payment-server에 의해 이미 호출되어 PAID로 바뀐 상태)
	s.mu.Lock()
	status := s.orders[orderID]
	s.mu.Unlock()
	log.Printf("[order] 주문 %s 완료: 결제승인=%v(%s), 최종상태=%s",
		orderID, payRes.GetApproved(), payRes.GetTransactionId(), status)

	return &orderpb.CreateOrderReply{OrderId: orderID, Status: status}, nil
}

// UpdateOrderStatus: payment-server가 결제 완료 후 이 메서드를 호출합니다.
// (order-server가 server 역할, payment-server가 client 역할)
func (s *orderServer) UpdateOrderStatus(ctx context.Context, req *orderpb.UpdateOrderStatusRequest) (*orderpb.UpdateOrderStatusReply, error) {
	s.mu.Lock()
	s.orders[req.GetOrderId()] = req.GetStatus()
	s.mu.Unlock()
	log.Printf("[order]   ← payment-server가 상태 갱신 요청: %s = %s", req.GetOrderId(), req.GetStatus())
	return &orderpb.UpdateOrderStatusReply{Ok: true}, nil
}

func main() {
	// payment-server를 호출할 클라이언트 준비.
	// grpc.NewClient는 lazy 연결이라, payment-server가 아직 안 떠 있어도 여기선 에러가 안 납니다.
	payConn, err := grpc.NewClient("localhost:50052",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[order] payment 연결 준비 실패: %v", err)
	}
	defer payConn.Close()

	srv := &orderServer{
		payment: paymentpb.NewPaymentServiceClient(payConn),
		orders:  make(map[string]string),
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("[order] 포트 열기 실패: %v", err)
	}

	g := grpc.NewServer()
	orderpb.RegisterOrderServiceServer(g, srv)
	log.Println("[order] gRPC 서버 시작 :50051")
	if err := g.Serve(lis); err != nil {
		log.Fatalf("[order] 서버 실행 실패: %v", err)
	}
}
