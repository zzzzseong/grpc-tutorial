// order-server — 주문 서버

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

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

// CreateBulkOrder [3단계: Client Streaming] 장바구니 주문.
//
// 2단계와 입장이 뒤집혔습니다:
//   - 이번엔 서버인 내가 stream.Recv()로 여러 개를 받고,
//   - 다 받으면 SendAndClose()로 응답 1개를 보내며 마무리합니다.
//
// 시그니처도 특이합니다: 요청 파라미터가 아예 없습니다.
// 요청들이 stream 안에서 하나씩 나오기 때문입니다.
func (s *orderServer) CreateBulkOrder(stream orderpb.OrderService_CreateBulkOrderServer) error {
	var (
		items []string
		total int32
	)

	// 1. 클라이언트가 보내는 상품을 하나씩 수신.
	//    클라이언트가 "다 보냈다"(CloseAndRecv)고 하면 io.EOF가 옵니다.
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break // 클라이언트 송신 종료 → 이제 응답할 차례
		}
		if err != nil {
			return err
		}
		items = append(items, item.GetItem())
		total += item.GetAmount()
		log.Printf("[order] (bulk)   상품 수신: %s (%d원), 누적 %d원", item.GetItem(), item.GetAmount(), total)
	}

	// 2. 다 받았으니 주문 1건으로 합쳐서 생성
	s.mu.Lock()
	s.seq++
	orderID := fmt.Sprintf("ORD-%03d", s.seq)
	s.orders[orderID] = "CREATED"
	s.mu.Unlock()
	log.Printf("[order] (bulk) 주문 생성 %s: 상품 %d개, 합계 %d원 → 결제 요청", orderID, len(items), total)

	// 3. 합산 금액으로 결제 (1단계 unary rpc 재사용)
	if _, err := s.payment.ProcessPayment(stream.Context(), &paymentpb.ProcessPaymentRequest{
		OrderId: orderID,
		Amount:  total,
	}); err != nil {
		return err
	}

	s.mu.Lock()
	status := s.orders[orderID]
	s.mu.Unlock()

	// 4. SendAndClose: 응답 1개를 보내면서 스트림을 닫습니다.
	//    (client streaming에서만 쓰는 전용 메서드)
	return stream.SendAndClose(&orderpb.CreateBulkOrderReply{
		OrderId:     orderID,
		Status:      status,
		ItemCount:   int32(len(items)),
		TotalAmount: total,
	})
}

// OrderSession [4단계: Bidirectional Streaming] 주문 세션.
//
// 요청 스트림과 응답 스트림이 독립적으로 흐르는 전이중 통신입니다.
// "요청 하나 받고 → 응답 하나 보내고"의 핑퐁이 아니라,
// 수신 루프는 계속 돌면서 주문마다 goroutine을 띄워 병렬 처리하고,
// 각 goroutine이 끝나는 순서대로 결과를 Send합니다.
// → 그래서 결과가 보낸 순서와 다르게 도착할 수 있습니다.
func (s *orderServer) OrderSession(stream orderpb.OrderService_OrderSessionServer) error {
	var (
		wg     sync.WaitGroup
		sendMu sync.Mutex // stream.Send는 동시 호출이 금지되어 있어 뮤텍스로 직렬화합니다
	)

	// 수신 루프: 클라이언트가 CloseSend할 때까지 주문을 계속 받습니다.
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break // 클라이언트 송신 종료. 단, 처리 중인 주문이 남아 있을 수 있음
		}
		if err != nil {
			return err
		}

		// 주문 생성
		s.mu.Lock()
		s.seq++
		orderID := fmt.Sprintf("ORD-%03d", s.seq)
		s.orders[orderID] = "CREATED"
		s.mu.Unlock()
		log.Printf("[order] (session) 주문 수신 %s (%s, %d원) → 병렬 처리 시작", orderID, req.GetItem(), req.GetAmount())

		// 주문마다 goroutine으로 병렬 처리. 수신 루프는 기다리지 않고 다음 Recv로 갑니다.
		wg.Add(1)
		go func(orderID string, req *orderpb.CreateOrderRequest) {
			defer wg.Done()

			// 금액이 클수록 심사가 오래 걸린다고 가정 (결과가 순서 없이 도착하는 걸 보여주기 위한 장치)
			time.Sleep(time.Duration(req.GetAmount()) * time.Microsecond * 300)

			// 결제는 1단계 unary rpc 재사용
			if _, err := s.payment.ProcessPayment(stream.Context(), &paymentpb.ProcessPaymentRequest{
				OrderId: orderID,
				Amount:  req.GetAmount(),
			}); err != nil {
				log.Printf("[order] (session) %s 결제 실패: %v", orderID, err)
				return
			}

			s.mu.Lock()
			status := s.orders[orderID]
			s.mu.Unlock()

			sendMu.Lock()
			err := stream.Send(&orderpb.OrderResult{OrderId: orderID, Item: req.GetItem(), Status: status})
			sendMu.Unlock()
			if err != nil {
				log.Printf("[order] (session) %s 결과 전송 실패: %v", orderID, err)
				return
			}
			log.Printf("[order] (session) %s (%s) 완료 → 결과 전송", orderID, req.GetItem())
		}(orderID, req)
	}

	// 아직 처리 중인 주문들이 결과를 다 보낼 때까지 기다린 뒤 스트림을 닫습니다.
	wg.Wait()
	log.Printf("[order] (session) 세션 종료")
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
