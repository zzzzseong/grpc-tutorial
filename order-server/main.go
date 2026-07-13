// order-server — 주문 서버.
//
// 이 프로세스 하나가 두 가지 역할을 동시에 합니다:
//   - server 역할: OrderService를 구현하고 :50051 포트를 엽니다.
//   - client 역할: payment-server의 PaymentService 스텁을 들고 결제를 호출합니다.
//
// 즉 "gRPC 서버 = 남을 못 부른다"가 아니라, 한 서버가 서버이자 클라이언트입니다.
package main

import (
	"context"
	"fmt"
	"io"
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

// CreateOrder 외부에서 주문 생성을 요청하면 실행 (order-server가 server 역할).
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

// CreateOrderWithProgress [2단계: Server Streaming] CreateOrder의 스트리밍 버전.
//
// 이 함수는 스트리밍의 양쪽 입장을 동시에 보여줍니다:
//   - 서버로서: stream.Send()로 호출자(trigger)에게 진행 상황을 흘려보냄
//   - 클라이언트로서: payStream.Recv()로 payment-server의 스트림을 받아옴
//
// 즉 payment의 진행 상황을 받는 족족 내 호출자에게 릴레이하는 구조입니다.
func (s *orderServer) CreateOrderWithProgress(req *orderpb.CreateOrderRequest, stream orderpb.OrderService_CreateOrderWithProgressServer) error {
	// 1. 주문 생성 (unary 버전과 동일)
	s.mu.Lock()
	s.seq++
	orderID := fmt.Sprintf("ORD-%03d", s.seq)
	s.orders[orderID] = "CREATED"
	s.mu.Unlock()

	// 첫 조각 전송: 주문 생성됨
	if err := stream.Send(&orderpb.OrderProgress{
		OrderId: orderID, Stage: "CREATED",
		Message: fmt.Sprintf("주문 접수 (%s, %d원)", req.GetItem(), req.GetAmount()),
	}); err != nil {
		return err
	}
	log.Printf("[order] (stream) 주문 생성 %s → payment 스트림 구독 시작", orderID)

	// 2. payment-server의 스트리밍 rpc 호출.
	//    unary와 달리 응답이 바로 오지 않고 "수신용 스트림"이 돌아옵니다.
	payStream, err := s.payment.ProcessPaymentStream(stream.Context(), &paymentpb.ProcessPaymentRequest{
		OrderId: orderID,
		Amount:  req.GetAmount(),
	})
	if err != nil {
		return err
	}

	// 3. 스트림 수신 루프. 서버가 return할 때까지 Recv()가 조각을 하나씩 꺼내주고,
	//    스트림이 끝나면 io.EOF가 옵니다. 이 패턴은 정해진 관용구입니다.
	for {
		prog, err := payStream.Recv()
		if err == io.EOF {
			break // payment-server가 스트림을 정상 종료함
		}
		if err != nil {
			return err
		}

		// 받은 조각을 내 호출자에게 릴레이
		msg := "결제 진행 중"
		if prog.GetStage() == "APPROVED" {
			msg = "결제 승인 " + prog.GetTransactionId()
		}
		if err := stream.Send(&orderpb.OrderProgress{
			OrderId: orderID, Stage: "PAYMENT_" + prog.GetStage(), Message: msg,
		}); err != nil {
			return err
		}
		log.Printf("[order] (stream)   payment %s 수신 → 릴레이", prog.GetStage())
	}

	// 4. 마지막 조각: 최종 주문 상태
	//    (스트림 도중 payment-server가 UpdateOrderStatus를 호출해 PAID로 바뀐 상태)
	s.mu.Lock()
	status := s.orders[orderID]
	s.mu.Unlock()
	if err := stream.Send(&orderpb.OrderProgress{
		OrderId: orderID, Stage: status, Message: "주문 처리 완료",
	}); err != nil {
		return err
	}
	log.Printf("[order] (stream) 주문 %s 완료: 최종상태=%s", orderID, status)

	return nil
}

// UpdateOrderStatus payment-server가 결제 완료 후 이 메서드를 호출합니다.
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
