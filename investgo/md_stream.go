package investgo

import (
	"context"
	"fmt"
	"strings"
	"sync"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type subscriptionStatus int64

const (
	subscriptionRequested subscriptionStatus = iota
	subscriptionSuccess
	unsubscriptionRequested
)

type (
	MarketDataStream struct {
		stream pb.MarketDataStreamService_MarketDataStreamClient
		ctx    context.Context
		cancel context.CancelFunc
		logger Logger

		subscriptionCandleResponseChannel        chan *pb.SubscribeCandlesResponse
		subscriptionOrderBookResponseChannel     chan *pb.SubscribeOrderBookResponse
		subscriptionTradesResponseChannel        chan *pb.SubscribeTradesResponse
		subscriptionLastPriceResponseChannel     chan *pb.SubscribeLastPriceResponse
		subscriptionTradingStatusResponseChannel chan *pb.SubscribeInfoResponse

		candle        chan *pb.Candle
		trade         chan *pb.Trade
		orderBook     chan *pb.OrderBook
		lastPrice     chan *pb.LastPrice
		tradingStatus chan *pb.TradingStatus

		candleSubscriptions        candleKeyStatusRegistry
		orderBookSubscriptions     orderBookStatusRegistry
		tradesSubscriptions        strStatusRegistry
		tradingStatusSubscriptions strStatusRegistry
		lastPriceSubscriptions     strStatusRegistry
	}

	strStatusRegistry struct {
		sync.RWMutex
		registry map[string]subscriptionStatus
	}

	orderBookStatusRegistry struct {
		sync.RWMutex
		registry map[orderBooksKey]subscriptionStatus
	}

	candleKeyStatusRegistry struct {
		sync.RWMutex
		registry map[candleKey]subscriptionStatus
	}

	candleKey struct {
		instrumentId string
		interval     pb.SubscriptionInterval
		waitingClose bool
	}

	orderBooksKey struct {
		instrumentId string
		depth        int32
	}
)

func (ck candleKey) ToString() string {
	return fmt.Sprintf("%s,%s,%t", ck.instrumentId, pb.SubscriptionInterval_name[int32(ck.interval)], ck.waitingClose)
}

func (obk orderBooksKey) ToString() string {
	return fmt.Sprintf("%s,%d", obk.instrumentId, obk.depth)
}

// Общий канал для свечей на которые подписаны, для параллельной прослушки сообщений используйте паттерн брокер
func (mds *MarketDataStream) GetCandlesChannel() <-chan *pb.Candle {
	return mds.candle
}

// Канал с результатами подписки отписки на свечи
func (mds *MarketDataStream) GetSubscriptionCandleResponseChannel() <-chan *pb.SubscribeCandlesResponse {
	return mds.subscriptionCandleResponseChannel
}

// Общий канал для стаканов на которые подписаны, для параллельной прослушки сообщений используйте паттерн брокер
func (mds *MarketDataStream) GetOrderBooksChannel() <-chan *pb.OrderBook {
	return mds.orderBook
}

// Канал с результатами подписки отписки на стакан
func (mds *MarketDataStream) GetSubscriptionOrderBookResponseChannel() <-chan *pb.SubscribeOrderBookResponse {
	return mds.subscriptionOrderBookResponseChannel
}

// Общий канал для сделок на которые подписаны, для параллельной прослушки сообщений используйте паттерн брокер
func (mds *MarketDataStream) GetTradesChannel() <-chan *pb.Trade {
	return mds.trade
}

// Канал с результатами подписки отписки на сделки
func (mds *MarketDataStream) GetSubscriptionTradesResponseChannel() <-chan *pb.SubscribeTradesResponse {
	return mds.subscriptionTradesResponseChannel
}

// Общий канал для последних цен на которые подписаны, для параллельной прослушки сообщений используйте паттерн брокер
func (mds *MarketDataStream) GetLastPricesChannel() <-chan *pb.LastPrice {
	return mds.lastPrice
}

// Канал с результатами подписки отписки на последнюю цену
func (mds *MarketDataStream) GetSubscriptionLastPricesResponseChannel() <-chan *pb.SubscribeLastPriceResponse {
	return mds.subscriptionLastPriceResponseChannel
}

// Общий канал для статусов торговых инструментов на которые подписаны, для параллельной прослушки сообщений используйте паттерн брокер
func (mds *MarketDataStream) GetTradingStatusesChannel() <-chan *pb.TradingStatus {
	return mds.tradingStatus
}

// Канал с результатами подписки отписки на статусы торговых инструментов
func (mds *MarketDataStream) GetSubscriptionTradingStatusesResponseChannel() <-chan *pb.SubscribeInfoResponse {
	return mds.subscriptionTradingStatusResponseChannel
}

func (mds *MarketDataStream) SubscribeCandle(instrumentIds []string, interval pb.SubscriptionInterval, waitingClose bool) error {
	mds.candleSubscriptions.Lock()
	defer mds.candleSubscriptions.Unlock()
	for _, id := range instrumentIds {
		var wc bool
		if interval != pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE {
			wc = true
		} else {
			wc = waitingClose
		}
		key := candleKey{instrumentId: id, interval: interval, waitingClose: wc}
		status, ok := mds.candleSubscriptions.registry[key]
		if ok {
			return fmt.Errorf("неожиданное состояние подписки на свечи при подписке %s: %d", key.ToString(), status)
		}
	}
	err := mds.sendCandlesReq(instrumentIds, interval, pb.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE, waitingClose)
	if err != nil {
		return fmt.Errorf("не удалось подписаться на свечи %s, %s, %t: %s",
			strings.Join(instrumentIds, ","), pb.SubscriptionInterval_name[int32(interval)], waitingClose, err.Error())
	}

	for _, id := range instrumentIds {
		var wc bool
		if interval != pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE {
			wc = true
		} else {
			wc = waitingClose
		}
		key := candleKey{instrumentId: id, interval: interval, waitingClose: wc}
		mds.candleSubscriptions.registry[key] = subscriptionRequested
	}
	return nil
}

func (mds *MarketDataStream) UnSubscribeCandle(instrumentIds []string, interval pb.SubscriptionInterval, waitingClose bool) error {
	mds.candleSubscriptions.Lock()
	defer mds.candleSubscriptions.Unlock()
	for _, id := range instrumentIds {
		var wc bool
		if interval != pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE {
			wc = true
		} else {
			wc = waitingClose
		}
		key := candleKey{instrumentId: id, interval: interval, waitingClose: wc}
		status, ok := mds.candleSubscriptions.registry[key]
		if !ok || status != subscriptionSuccess {
			return fmt.Errorf("неожиданное состояние подписки на свечи при отписке %s: %d", key.ToString(), status)
		}
	}
	err := mds.sendCandlesReq(instrumentIds, interval, pb.SubscriptionAction_SUBSCRIPTION_ACTION_UNSUBSCRIBE, waitingClose)
	if err != nil {
		return fmt.Errorf("не удалось отписаться от свечи %s, %s, %t: %s",
			strings.Join(instrumentIds, ","), pb.SubscriptionInterval_name[int32(interval)], waitingClose, err.Error())
	}
	for _, id := range instrumentIds {
		var wc bool
		if interval != pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE {
			wc = true
		} else {
			wc = waitingClose
		}
		key := candleKey{instrumentId: id, interval: interval, waitingClose: wc}
		mds.candleSubscriptions.registry[key] = unsubscriptionRequested
	}
	return nil
}

func (mds *MarketDataStream) handleSubscribeCandlesResponse(resp *pb.SubscribeCandlesResponse) {
	// это сообщение приходит не только как результат отписки/подписки, но и в ответ на get_my_subscriptions
	select {
	case mds.subscriptionCandleResponseChannel <- resp:
	default:
	}
	mds.candleSubscriptions.Lock()
	defer mds.candleSubscriptions.Unlock()
	for _, sub := range resp.GetCandlesSubscriptions() {
		// TODO как различить подписки на минутки с закрытыми и открытыми свечами?
		mds.logger.Debugf("статус подписки на свечи %s: %s",
			candleKey{instrumentId: sub.InstrumentUid, interval: sub.Interval}.ToString(),
			pb.SubscriptionStatus_name[int32(sub.SubscriptionStatus)])

		switch sub.SubscriptionStatus {
		case pb.SubscriptionStatus_SUBSCRIPTION_STATUS_SUCCESS:
			// нет способа различить статус подписки или отписки
			if sub.Interval == pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE {
				// будем гадать, тк не знаем из сообщения значение waitingClose
				key1 := candleKey{instrumentId: sub.InstrumentUid, interval: sub.Interval, waitingClose: true}
				status1, ok1 := mds.candleSubscriptions.registry[key1]
				key2 := candleKey{instrumentId: sub.InstrumentUid, interval: sub.Interval, waitingClose: false}
				status2, ok2 := mds.candleSubscriptions.registry[key2]
				if ok1 && ok2 {
					if status1 == subscriptionSuccess && status2 != subscriptionSuccess {
						if status2 == subscriptionRequested {
							mds.candleSubscriptions.registry[key2] = subscriptionSuccess
						} else if status2 == unsubscriptionRequested {
							delete(mds.candleSubscriptions.registry, key2)
						}
					} else if status1 != subscriptionSuccess && status2 == subscriptionSuccess {
						if status1 == subscriptionRequested {
							mds.candleSubscriptions.registry[key1] = subscriptionSuccess
						} else if status1 == unsubscriptionRequested {
							delete(mds.candleSubscriptions.registry, key1)
						}
					} else {
						mds.logger.Warnf("невозможно определить для какой минутной подписки пришел статус!")
						continue
					}
				} else {
					if ok1 {
						if status1 == subscriptionRequested {
							mds.candleSubscriptions.registry[key1] = subscriptionSuccess
						} else if status1 == unsubscriptionRequested {
							delete(mds.candleSubscriptions.registry, key1)
						}
					} else {
						if status2 == subscriptionRequested {
							mds.candleSubscriptions.registry[key2] = subscriptionSuccess
						} else if status2 == unsubscriptionRequested {
							delete(mds.candleSubscriptions.registry, key2)
						}
					}
				}
			} else {
				key := candleKey{instrumentId: sub.InstrumentUid, interval: sub.Interval, waitingClose: true}
				status, ok := mds.candleSubscriptions.registry[key]
				if !ok {
					mds.logger.Warnf("неожиданное состояние подписки на свечи при обработке статуса %s", key.ToString())
				} else {
					if status == subscriptionRequested {
						mds.candleSubscriptions.registry[key] = subscriptionSuccess
					} else if status == unsubscriptionRequested {
						delete(mds.candleSubscriptions.registry, key)
					}
				}
			}
		default:
			// нет способа различить статус подписки или отписки
			if sub.Interval == pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE {
				// будем гадать, тк не знаем из сообщения значение waitingClose
				key1 := candleKey{instrumentId: sub.InstrumentUid, interval: sub.Interval, waitingClose: true}
				status1, ok1 := mds.candleSubscriptions.registry[key1]
				key2 := candleKey{instrumentId: sub.InstrumentUid, interval: sub.Interval, waitingClose: false}
				status2, ok2 := mds.candleSubscriptions.registry[key2]
				if ok1 && ok2 {
					if status1 == subscriptionSuccess && status2 != subscriptionSuccess {
						if status2 == subscriptionRequested {
							delete(mds.candleSubscriptions.registry, key2)
						} else if status2 == unsubscriptionRequested {
							mds.candleSubscriptions.registry[key2] = subscriptionSuccess
						}
					} else if status1 != subscriptionSuccess && status2 == subscriptionSuccess {
						if status1 == subscriptionRequested {
							delete(mds.candleSubscriptions.registry, key1)
						} else if status1 == unsubscriptionRequested {
							mds.candleSubscriptions.registry[key1] = subscriptionSuccess
						}
					} else {
						mds.logger.Warnf("невозможно определить для какой минутной подписки пришел статус!")
						continue
					}
				} else {
					if ok1 {
						if status1 == subscriptionRequested {
							delete(mds.candleSubscriptions.registry, key1)
						} else if status1 == unsubscriptionRequested {
							mds.candleSubscriptions.registry[key1] = subscriptionSuccess
						}
					} else {
						if status2 == subscriptionRequested {
							delete(mds.candleSubscriptions.registry, key2)
						} else if status2 == unsubscriptionRequested {
							mds.candleSubscriptions.registry[key2] = subscriptionSuccess
						}
					}
				}
			} else {
				key := candleKey{instrumentId: sub.InstrumentUid, interval: sub.Interval, waitingClose: true}
				status, ok := mds.candleSubscriptions.registry[key]
				if !ok {
					mds.logger.Warnf("неожиданное состояние подписки на свечи при обработке статуса %s", key.ToString())
				} else {
					if status == subscriptionRequested {
						delete(mds.candleSubscriptions.registry, key)
					} else if status == unsubscriptionRequested {
						mds.candleSubscriptions.registry[key] = subscriptionSuccess
					}
				}
			}
		}
	}
}

func (mds *MarketDataStream) UnSubscribeAllCandles() error {
	mds.candleSubscriptions.Lock()
	keys := make([]candleKey, 0, len(mds.candleSubscriptions.registry))
	for key := range mds.candleSubscriptions.registry {
		keys = append(keys, key)
	}
	mds.candleSubscriptions.Unlock()
	for _, key := range keys {
		err := mds.UnSubscribeCandle([]string{key.instrumentId}, key.interval, key.waitingClose)
		if err != nil {
			return fmt.Errorf("ошибка при попытке отписаться от свечи: %s", key.ToString())
		}
	}
	return nil
}

func (mds *MarketDataStream) UnSubscribeAllOrderBooks() error {
	mds.orderBookSubscriptions.Lock()
	keys := make([]orderBooksKey, 0, len(mds.orderBookSubscriptions.registry))
	for key := range mds.orderBookSubscriptions.registry {
		keys = append(keys, key)
	}
	mds.orderBookSubscriptions.Unlock()
	for _, key := range keys {
		err := mds.UnSubscribeOrderBook([]string{key.instrumentId}, key.depth)
		if err != nil {
			return fmt.Errorf("ошибка при попытке отписаться от стакана: %s", key.ToString())
		}
	}
	return nil
}

func (mds *MarketDataStream) UnSubscribeAllTrades() error {
	mds.tradesSubscriptions.Lock()
	keys := make([]string, 0, len(mds.tradesSubscriptions.registry))
	for key := range mds.tradesSubscriptions.registry {
		keys = append(keys, key)
	}
	mds.tradesSubscriptions.Unlock()
	for _, key := range keys {
		err := mds.UnSubscribeTrades([]string{key})
		if err != nil {
			return fmt.Errorf("ошибка при попытке отписаться от сделок: %s", key)
		}
	}
	return nil
}

func (mds *MarketDataStream) UnSubscribeAllLastPrices() error {
	mds.lastPriceSubscriptions.Lock()
	keys := make([]string, 0, len(mds.lastPriceSubscriptions.registry))
	for key := range mds.lastPriceSubscriptions.registry {
		keys = append(keys, key)
	}
	mds.lastPriceSubscriptions.Unlock()
	for _, key := range keys {
		err := mds.UnSubscribeLastPrice([]string{key})
		if err != nil {
			return fmt.Errorf("ошибка при попытке отписаться от последних цен: %s", key)
		}
	}
	return nil
}

func (mds *MarketDataStream) UnSubscribeAllTradingStatuses() error {
	mds.tradingStatusSubscriptions.Lock()
	keys := make([]string, 0, len(mds.tradingStatusSubscriptions.registry))
	for key := range mds.tradingStatusSubscriptions.registry {
		keys = append(keys, key)
	}
	mds.tradingStatusSubscriptions.Unlock()
	for _, key := range keys {
		err := mds.UnSubscribeTradingStatus([]string{key})
		if err != nil {
			return fmt.Errorf("ошибка при попытке отписаться от статусов торгового инструмента: %s", key)
		}
	}
	return nil
}

func (mds *MarketDataStream) UnSubscribeAll() error {
	if err := mds.UnSubscribeAllCandles(); err != nil {
		return err
	}
	if err := mds.UnSubscribeAllLastPrices(); err != nil {
		return err
	}
	if err := mds.UnSubscribeAllOrderBooks(); err != nil {
		return err
	}
	if err := mds.UnSubscribeAllTrades(); err != nil {
		return err
	}
	if err := mds.UnSubscribeAllTradingStatuses(); err != nil {
		return err
	}
	return nil
}

func (mds *MarketDataStream) sendCandlesReq(instrumentIds []string, interval pb.SubscriptionInterval, act pb.SubscriptionAction, waitingClose bool) error {
	instruments := make([]*pb.CandleInstrument, 0, len(instrumentIds))
	for _, id := range instrumentIds {
		instruments = append(instruments, &pb.CandleInstrument{
			InstrumentId: id,
			Interval:     interval,
		})
	}

	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeCandlesRequest{
			SubscribeCandlesRequest: &pb.SubscribeCandlesRequest{
				SubscriptionAction: act,
				Instruments:        instruments,
				WaitingClose:       waitingClose,
			}}})
}

func (mds *MarketDataStream) RequestMySubscriptions() error {
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_GetMySubscriptions{
			GetMySubscriptions: &pb.GetMySubscriptions{}}})
}

func (mds *MarketDataStream) SubscribeOrderBook(instrumentIds []string, depth int32) error {
	mds.orderBookSubscriptions.Lock()
	defer mds.orderBookSubscriptions.Unlock()
	for _, id := range instrumentIds {
		key := orderBooksKey{instrumentId: id, depth: depth}
		status, ok := mds.orderBookSubscriptions.registry[key]
		if ok {
			return fmt.Errorf("неожиданное состояние подписки на стакан при подписке %s: %d", key.ToString(), status)
		}
	}
	err := mds.sendOrderBookReq(instrumentIds, depth, pb.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE)
	if err != nil {
		return fmt.Errorf("не удалось подписаться на стакан %s, %d: %s",
			strings.Join(instrumentIds, ","), depth, err.Error())
	}
	for _, id := range instrumentIds {
		key := orderBooksKey{instrumentId: id, depth: depth}
		mds.orderBookSubscriptions.registry[key] = subscriptionRequested
	}
	return nil
}

func (mds *MarketDataStream) UnSubscribeOrderBook(instrumentIds []string, depth int32) error {
	mds.orderBookSubscriptions.Lock()
	defer mds.orderBookSubscriptions.Unlock()
	for _, id := range instrumentIds {
		key := orderBooksKey{instrumentId: id, depth: depth}
		status, ok := mds.orderBookSubscriptions.registry[key]
		if !ok || status != subscriptionSuccess {
			return fmt.Errorf("неожиданное состояние подписки на стакан при отписке %s: %d", key.ToString(), status)
		}
	}
	err := mds.sendOrderBookReq(instrumentIds, depth, pb.SubscriptionAction_SUBSCRIPTION_ACTION_UNSUBSCRIBE)
	if err != nil {
		return fmt.Errorf("не удалось отписаться от стакана %s, %d: %s",
			strings.Join(instrumentIds, ","), depth, err.Error())
	}
	for _, id := range instrumentIds {
		key := orderBooksKey{instrumentId: id, depth: depth}
		mds.orderBookSubscriptions.registry[key] = unsubscriptionRequested
	}
	return nil
}

func (mds *MarketDataStream) sendOrderBookReq(instrumentIds []string, depth int32, act pb.SubscriptionAction) error {
	instruments := make([]*pb.OrderBookInstrument, 0, len(instrumentIds))
	for _, id := range instrumentIds {
		instruments = append(instruments, &pb.OrderBookInstrument{
			Depth:        depth,
			InstrumentId: id,
		})
	}
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeOrderBookRequest{
			SubscribeOrderBookRequest: &pb.SubscribeOrderBookRequest{
				SubscriptionAction: act,
				Instruments:        instruments,
			}}})
}

func (mds *MarketDataStream) handleSubscribeOrderBooksResponse(resp *pb.SubscribeOrderBookResponse) {
	// это сообщение приходит не только как результат отписки/подписки, но и в ответ на get_my_subscriptions
	select {
	case mds.subscriptionOrderBookResponseChannel <- resp:
	default:
	}
	mds.orderBookSubscriptions.Lock()
	defer mds.orderBookSubscriptions.Unlock()

	for _, sub := range resp.GetOrderBookSubscriptions() {
		key := orderBooksKey{instrumentId: sub.InstrumentUid, depth: sub.Depth}
		mds.logger.Debugf("статус подписки на стакан %s: %s", key, pb.SubscriptionAction_name[int32(sub.SubscriptionStatus)])

		switch sub.SubscriptionStatus {
		case pb.SubscriptionStatus_SUBSCRIPTION_STATUS_SUCCESS:
			status, ok := mds.orderBookSubscriptions.registry[key]
			if !ok {
				mds.logger.Warnf("неожиданное состояние подписки на стакан при обработке статуса %s", key)
			} else {
				if status == subscriptionRequested {
					mds.orderBookSubscriptions.registry[key] = subscriptionSuccess
				} else if status == unsubscriptionRequested {
					delete(mds.orderBookSubscriptions.registry, key)
				}
			}
		default:
			status, ok := mds.orderBookSubscriptions.registry[key]
			if !ok {
				mds.logger.Warnf("неожиданное состояние подписки на стакан при обработке статуса %s", key)
			} else {
				if status == subscriptionRequested {
					delete(mds.orderBookSubscriptions.registry, key)
				} else if status == unsubscriptionRequested {
					mds.orderBookSubscriptions.registry[key] = subscriptionSuccess
				}
			}
		}
	}
}

func (mds *MarketDataStream) SubscribeTrades(instrumentIds []string) error {
	mds.tradesSubscriptions.Lock()
	defer mds.tradesSubscriptions.Unlock()
	for _, id := range instrumentIds {
		status, ok := mds.tradesSubscriptions.registry[id]
		if ok {
			return fmt.Errorf("неожиданное состояние подписки на сделки при подписке %s: %d", id, status)
		}
	}
	err := mds.sendTradesReq(instrumentIds, pb.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE)
	if err != nil {
		return fmt.Errorf("не удалось подписаться на сделки %s: %s",
			strings.Join(instrumentIds, ","), err.Error())
	}
	for _, id := range instrumentIds {
		mds.tradesSubscriptions.registry[id] = subscriptionRequested
	}
	return nil
}

func (mds *MarketDataStream) UnSubscribeTrades(instrumentIds []string) error {
	mds.tradesSubscriptions.Lock()
	defer mds.tradesSubscriptions.Unlock()
	for _, id := range instrumentIds {
		status, ok := mds.tradesSubscriptions.registry[id]
		if !ok || status != subscriptionSuccess {
			return fmt.Errorf("неожиданное состояние подписки на сделки при отписке %s: %d", id, status)
		}
	}
	err := mds.sendTradesReq(instrumentIds, pb.SubscriptionAction_SUBSCRIPTION_ACTION_UNSUBSCRIBE)
	if err != nil {
		return fmt.Errorf("не удалось отписаться от сделок %s: %s",
			strings.Join(instrumentIds, ","), err.Error())
	}
	for _, id := range instrumentIds {
		mds.tradesSubscriptions.registry[id] = unsubscriptionRequested
	}
	return nil
}

func (mds *MarketDataStream) sendTradesReq(instrumentIds []string, act pb.SubscriptionAction) error {
	instruments := make([]*pb.TradeInstrument, 0, len(instrumentIds))
	for _, id := range instrumentIds {
		instruments = append(instruments, &pb.TradeInstrument{
			InstrumentId: id,
		})
	}
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeTradesRequest{
			SubscribeTradesRequest: &pb.SubscribeTradesRequest{
				SubscriptionAction: act,
				Instruments:        instruments,
			}}})
}

func (mds *MarketDataStream) handleSubscribeTradesResponse(resp *pb.SubscribeTradesResponse) {
	// это сообщение приходит не только как результат отписки/подписки, но и в ответ на get_my_subscriptions
	select {
	case mds.subscriptionTradesResponseChannel <- resp:
	default:
	}
	mds.tradesSubscriptions.Lock()
	defer mds.tradesSubscriptions.Unlock()
	for _, sub := range resp.GetTradeSubscriptions() {
		mds.logger.Debugf("статус подписки на сделки %s: %s", sub.InstrumentUid, pb.SubscriptionAction_name[int32(sub.SubscriptionStatus)])
		switch sub.SubscriptionStatus {
		case pb.SubscriptionStatus_SUBSCRIPTION_STATUS_SUCCESS:
			status, ok := mds.tradesSubscriptions.registry[sub.InstrumentUid]
			if !ok {
				mds.logger.Warnf("неожиданное состояние подписки на сделки при обработке статуса %s", sub.InstrumentUid)
			} else {
				if status == subscriptionRequested {
					mds.tradesSubscriptions.registry[sub.InstrumentUid] = subscriptionSuccess
				} else if status == unsubscriptionRequested {
					delete(mds.tradesSubscriptions.registry, sub.InstrumentUid)
				}
			}
		default:
			status, ok := mds.tradesSubscriptions.registry[sub.InstrumentUid]
			if !ok {
				mds.logger.Warnf("неожиданное состояние подписки на сделки при обработке статуса %s", sub.InstrumentUid)
			} else {
				if status == subscriptionRequested {
					delete(mds.tradesSubscriptions.registry, sub.InstrumentUid)
				} else if status == unsubscriptionRequested {
					mds.tradesSubscriptions.registry[sub.InstrumentUid] = subscriptionSuccess
				}
			}
		}
	}
}

func (mds *MarketDataStream) SubscribeLastPrice(instrumentIds []string) error {
	mds.lastPriceSubscriptions.Lock()
	defer mds.lastPriceSubscriptions.Unlock()
	for _, id := range instrumentIds {
		status, ok := mds.lastPriceSubscriptions.registry[id]
		if ok {
			return fmt.Errorf("неожиданное состояние подписки на поледние цены при подписке %s: %d", id, status)
		}
	}
	err := mds.sendLastPriceReq(instrumentIds, pb.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE)
	if err != nil {
		return fmt.Errorf("не удалось подписаться на последние цены %s: %s",
			strings.Join(instrumentIds, ","), err.Error())
	}
	for _, id := range instrumentIds {

		mds.lastPriceSubscriptions.registry[id] = subscriptionRequested
	}
	return nil
}

func (mds *MarketDataStream) UnSubscribeLastPrice(instrumentIds []string) error {
	mds.lastPriceSubscriptions.Lock()
	defer mds.lastPriceSubscriptions.Unlock()
	for _, id := range instrumentIds {
		status, ok := mds.lastPriceSubscriptions.registry[id]
		if !ok || status != subscriptionSuccess {
			return fmt.Errorf("неожиданное состояние подписки на последние цены при отписке %s: %d", id, status)
		}
	}
	err := mds.sendLastPriceReq(instrumentIds, pb.SubscriptionAction_SUBSCRIPTION_ACTION_UNSUBSCRIBE)
	if err != nil {
		return fmt.Errorf("не удалось отписаться от последних цен %s: %s",
			strings.Join(instrumentIds, ","), err.Error())
	}
	for _, id := range instrumentIds {
		mds.lastPriceSubscriptions.registry[id] = unsubscriptionRequested
	}
	return nil
}

func (mds *MarketDataStream) sendLastPriceReq(instrumentIds []string, act pb.SubscriptionAction) error {
	instruments := make([]*pb.LastPriceInstrument, 0, len(instrumentIds))
	for _, id := range instrumentIds {
		instruments = append(instruments, &pb.LastPriceInstrument{
			InstrumentId: id,
		})
	}
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeLastPriceRequest{
			SubscribeLastPriceRequest: &pb.SubscribeLastPriceRequest{
				SubscriptionAction: act,
				Instruments:        instruments,
			}}})
}

func (mds *MarketDataStream) handleSubscribeLastPriceResponse(resp *pb.SubscribeLastPriceResponse) {
	// это сообщение приходит не только как результат отписки/подписки, но и в ответ на get_my_subscriptions
	select {
	case mds.subscriptionLastPriceResponseChannel <- resp:
	default:
	}
	mds.lastPriceSubscriptions.Lock()
	defer mds.lastPriceSubscriptions.Unlock()
	for _, sub := range resp.GetLastPriceSubscriptions() {
		mds.logger.Debugf("статус подписки на последние цены %s: %s", sub.InstrumentUid, pb.SubscriptionAction_name[int32(sub.SubscriptionStatus)])

		switch sub.SubscriptionStatus {
		case pb.SubscriptionStatus_SUBSCRIPTION_STATUS_SUCCESS:
			status, ok := mds.lastPriceSubscriptions.registry[sub.InstrumentUid]
			if !ok {
				mds.logger.Warnf("неожиданное состояние подписки на последние цены при обработке статуса %s", sub.InstrumentUid)
			} else {
				if status == subscriptionRequested {
					mds.lastPriceSubscriptions.registry[sub.InstrumentUid] = subscriptionSuccess
				} else if status == unsubscriptionRequested {
					delete(mds.lastPriceSubscriptions.registry, sub.InstrumentUid)
				}
			}
		default:
			status, ok := mds.lastPriceSubscriptions.registry[sub.InstrumentUid]
			if !ok {
				mds.logger.Warnf("неожиданное состояние подписки на последние цены при обработке статуса %s", sub.InstrumentUid)
			} else {
				if status == subscriptionRequested {
					delete(mds.lastPriceSubscriptions.registry, sub.InstrumentUid)
				} else if status == unsubscriptionRequested {
					mds.lastPriceSubscriptions.registry[sub.InstrumentUid] = subscriptionSuccess
				}
			}
		}
	}
}

func (mds *MarketDataStream) SubscribeTradingStatus(instrumentIds []string) error {
	mds.tradingStatusSubscriptions.Lock()
	defer mds.tradingStatusSubscriptions.Unlock()
	for _, id := range instrumentIds {
		status, ok := mds.tradingStatusSubscriptions.registry[id]
		if ok {
			return fmt.Errorf("неожиданное состояние подписки на статусы торговых инструментов при подписке %s: %d", id, status)
		}
	}
	err := mds.sendTradingStatusReq(instrumentIds, pb.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE)
	if err != nil {
		return fmt.Errorf("не удалось подписаться на статусы торговых инструментов %s: %s",
			strings.Join(instrumentIds, ","), err.Error())
	}
	for _, id := range instrumentIds {
		mds.tradingStatusSubscriptions.registry[id] = subscriptionRequested
	}
	return nil
}

func (mds *MarketDataStream) UnSubscribeTradingStatus(instrumentIds []string) error {
	mds.tradingStatusSubscriptions.Lock()
	defer mds.tradingStatusSubscriptions.Unlock()
	for _, id := range instrumentIds {
		status, ok := mds.tradingStatusSubscriptions.registry[id]
		if !ok || status != subscriptionSuccess {
			return fmt.Errorf("неожиданное состояние подписки на статусы торговых инструментов при отписке %s: %d", id, status)
		}
	}
	err := mds.sendTradingStatusReq(instrumentIds, pb.SubscriptionAction_SUBSCRIPTION_ACTION_UNSUBSCRIBE)
	if err != nil {
		return fmt.Errorf("не удалось отписаться от статусов торговых инструментов %s: %s",
			strings.Join(instrumentIds, ","), err.Error())
	}
	for _, id := range instrumentIds {
		mds.tradingStatusSubscriptions.registry[id] = unsubscriptionRequested
	}
	return nil
}

func (mds *MarketDataStream) sendTradingStatusReq(instrumentIds []string, act pb.SubscriptionAction) error {
	instruments := make([]*pb.InfoInstrument, 0, len(instrumentIds))
	for _, id := range instrumentIds {
		instruments = append(instruments, &pb.InfoInstrument{
			InstrumentId: id,
		})
	}
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeInfoRequest{
			SubscribeInfoRequest: &pb.SubscribeInfoRequest{
				SubscriptionAction: act,
				Instruments:        instruments,
			}}})
}

func (mds *MarketDataStream) handleSubscribeTradingStatusResponse(resp *pb.SubscribeInfoResponse) {
	// это сообщение приходит не только как результат отписки/подписки, но и в ответ на get_my_subscriptions
	select {
	case mds.subscriptionTradingStatusResponseChannel <- resp:
	default:
	}
	mds.tradingStatusSubscriptions.Lock()
	defer mds.tradingStatusSubscriptions.Unlock()
	for _, sub := range resp.GetInfoSubscriptions() {
		mds.logger.Debugf("статус подписки на статусы торговых инструментов %s: %s", sub.InstrumentUid, pb.SubscriptionAction_name[int32(sub.SubscriptionStatus)])

		switch sub.SubscriptionStatus {
		case pb.SubscriptionStatus_SUBSCRIPTION_STATUS_SUCCESS:
			status, ok := mds.tradingStatusSubscriptions.registry[sub.InstrumentUid]
			if !ok {
				mds.logger.Warnf("неожиданное состояние подписки на статусы торговых инструментов при обработке статуса %s", sub.InstrumentUid)
			} else {
				if status == subscriptionRequested {
					mds.tradingStatusSubscriptions.registry[sub.InstrumentUid] = subscriptionSuccess
				} else if status == unsubscriptionRequested {
					delete(mds.tradingStatusSubscriptions.registry, sub.InstrumentUid)
				}
			}
		default:
			status, ok := mds.tradingStatusSubscriptions.registry[sub.InstrumentUid]
			if !ok {
				mds.logger.Warnf("неожиданное состояние подписки на статусы торговых инструментов при обработке статуса %s", sub.InstrumentUid)
			} else {
				if status == subscriptionRequested {
					delete(mds.tradesSubscriptions.registry, sub.InstrumentUid)
				} else if status == unsubscriptionRequested {
					mds.tradingStatusSubscriptions.registry[sub.InstrumentUid] = subscriptionSuccess
				}
			}
		}
	}
}

func (mds *MarketDataStream) Listen() error {
	defer mds.shutdown()
	for {
		select {
		case <-mds.ctx.Done():
			return mds.ctx.Err()
		default:
			resp, err := mds.stream.Recv()
			if err != nil {
				// если ошибка связана с завершением контекста, обрабатываем ее
				switch {
				case status.Code(err) == codes.Canceled:
					return nil
				default:
					return err
				}
			} else {
				// логика определения того что пришло и отправка информации в нужный канал
				mds.sendRespToChannel(resp)
			}
		}
	}
}

func (mds *MarketDataStream) sendRespToChannel(resp *pb.MarketDataResponse) {
	switch resp.GetPayload().(type) {
	case *pb.MarketDataResponse_Candle:
		select {
		case mds.candle <- resp.GetCandle():
		default:
		}

	case *pb.MarketDataResponse_Orderbook:
		select {
		case mds.orderBook <- resp.GetOrderbook():
		default:
		}

	case *pb.MarketDataResponse_Trade:
		select {
		case mds.trade <- resp.GetTrade():
		default:
		}

	case *pb.MarketDataResponse_LastPrice:
		select {
		case mds.lastPrice <- resp.GetLastPrice():
		default:
		}

	case *pb.MarketDataResponse_TradingStatus:
		select {
		case mds.tradingStatus <- resp.GetTradingStatus():
		default:
		}

	case *pb.MarketDataResponse_SubscribeCandlesResponse:
		candleResp := resp.GetSubscribeCandlesResponse()
		mds.handleSubscribeCandlesResponse(candleResp)

	case *pb.MarketDataResponse_SubscribeInfoResponse:
		infoResp := resp.GetSubscribeInfoResponse()
		mds.handleSubscribeTradingStatusResponse(infoResp)

	case *pb.MarketDataResponse_SubscribeLastPriceResponse:
		lastPriceResp := resp.GetSubscribeLastPriceResponse()
		mds.handleSubscribeLastPriceResponse(lastPriceResp)

	case *pb.MarketDataResponse_SubscribeOrderBookResponse:
		orderBookResp := resp.GetSubscribeOrderBookResponse()
		mds.handleSubscribeOrderBooksResponse(orderBookResp)

	case *pb.MarketDataResponse_SubscribeTradesResponse:
		orderTradesResp := resp.GetSubscribeTradesResponse()
		mds.handleSubscribeTradesResponse(orderTradesResp)

	default:
		mds.logger.Infof("сообщение из потока MarketDataStream: %s", resp.String())
	}
}

func (mds *MarketDataStream) shutdown() {
	mds.logger.Infof("закрываем поток MarketDataStream")

	mds.UnSubscribeAllCandles()

	close(mds.candle)
	close(mds.subscriptionCandleResponseChannel)
	close(mds.trade)
	close(mds.lastPrice)
	close(mds.orderBook)
	close(mds.tradingStatus)
}

func (mds *MarketDataStream) Stop() {
	mds.cancel()
}

func (mds *MarketDataStream) restart(_ context.Context, attempt uint, err error) {
	mds.logger.Infof("перезапускаем поток MarketDataStream err = %s, попытка = %v", err.Error(), attempt)
}
