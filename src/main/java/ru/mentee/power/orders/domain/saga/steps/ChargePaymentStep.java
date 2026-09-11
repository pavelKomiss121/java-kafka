package ru.mentee.power.orders.domain.saga.steps;

import org.springframework.stereotype.Component;
import ru.mentee.power.orders.domain.saga.OrderSagaContext;
import ru.mentee.power.orders.domain.saga.SagaStep;
import ru.mentee.power.orders.ports.outgoing.PaymentsClient;
import ru.mentee.power.orders.ports.outgoing.PricingClient;

import java.math.BigDecimal;

/**
 * pricingClient.fetchDiscount(...) переехал сюда из OrderConsumerUseCase
 * (MKAFKA-04..07) — расчёт скидки логически часть оплаты, а не отдельный шаг саги.
 */
@Component
public class ChargePaymentStep implements SagaStep<OrderSagaContext> {

    private final PricingClient pricingClient;
    private final PaymentsClient paymentsClient;

    public ChargePaymentStep(PricingClient pricingClient, PaymentsClient paymentsClient) {
        this.pricingClient = pricingClient;
        this.paymentsClient = paymentsClient;
    }

    @Override
    public void execute(OrderSagaContext context) {
        BigDecimal discount = pricingClient.fetchDiscount(context.orderId(), context.payload().region());
        context.setDiscount(discount);

        BigDecimal amountAfterDiscount = context.payload().amount()
                .multiply(BigDecimal.ONE.subtract(discount));
        var paymentId = paymentsClient.charge(context.orderId(), amountAfterDiscount);
        context.setPaymentId(paymentId);
    }

    @Override
    public void compensate(OrderSagaContext context) {
        if (context.paymentId() != null) {
            paymentsClient.refund(context.paymentId());
        }
    }

    @Override
    public String name() {
        return "ChargePayment";
    }
}