package investgo

import (
	"context"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"github.com/russianinvestments/invest-api-go-sdk/retry"
)

type MarketDataStreamClient struct {
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.MarketDataStreamServiceClient
}

// MarketDataStream - метод возвращает стрим биржевой информации
func (c *MarketDataStreamClient) MarketDataStream() (*MarketDataStream, error) {
	ctx, cancel := context.WithCancel(c.ctx)
	mds := &MarketDataStream{
		stream:                                   nil,
		ctx:                                      ctx,
		cancel:                                   cancel,
		subscriptionCandleResponseChannel:        make(chan *pb.SubscribeCandlesResponse, 1),
		subscriptionOrderBookResponseChannel:     make(chan *pb.SubscribeOrderBookResponse, 1),
		subscriptionTradesResponseChannel:        make(chan *pb.SubscribeTradesResponse, 1),
		subscriptionLastPriceResponseChannel:     make(chan *pb.SubscribeLastPriceResponse, 1),
		subscriptionTradingStatusResponseChannel: make(chan *pb.SubscribeInfoResponse, 1),

		candleSubscriptions:        candleKeyStatusRegistry{registry: make(map[candleKey]subscriptionStatus)},
		orderBookSubscriptions:     orderBookStatusRegistry{registry: make(map[orderBooksKey]subscriptionStatus)},
		tradesSubscriptions:        strStatusRegistry{registry: make(map[string]subscriptionStatus)},
		tradingStatusSubscriptions: strStatusRegistry{registry: make(map[string]subscriptionStatus)},
		lastPriceSubscriptions:     strStatusRegistry{registry: make(map[string]subscriptionStatus)},

		logger:        c.logger,
		candle:        make(chan *pb.Candle, 1),
		trade:         make(chan *pb.Trade, 1),
		orderBook:     make(chan *pb.OrderBook, 1),
		lastPrice:     make(chan *pb.LastPrice, 1),
		tradingStatus: make(chan *pb.TradingStatus, 1),
	}

	stream, err := c.pbClient.MarketDataStream(ctx, retry.WithOnRetryCallback(mds.restart))
	if err != nil {
		cancel()
		return nil, err
	}
	mds.stream = stream
	return mds, nil
}
