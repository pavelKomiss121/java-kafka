package ru.mentee.power.orders.domain.usecase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort.PlaceOrderCommand;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort.PlaceOrderResult;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderPersistencePort;
import ru.mentee.power.orders.ports.outgoing.OutboxStorePort;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class PlaceOrderUseCaseTest {

    @Mock
    OrderPersistencePort persistencePort;

    @Mock
    OutboxStorePort outboxStorePort;

    @InjectMocks
    PlaceOrderUseCase useCase;

    @Test
    void place_savesOrder_andAppendsOutboxEvent() {
        var cmd = new PlaceOrderCommand(
                UUID.randomUUID(),
                "EU",
                OrderPriority.HIGH,
                new BigDecimal("10.00"),
                List.of(line())
        );

        PlaceOrderResult result = useCase.place(cmd);

        assertEquals("QUEUED", result.status());
        verify(persistencePort).savePending(any(Order.class));

        ArgumentCaptor<OrderEventPayload> payloadCaptor = ArgumentCaptor.forClass(OrderEventPayload.class);
        verify(outboxStorePort).append(eq(result.orderId()), payloadCaptor.capture());
        assertEquals(OrderPriority.HIGH, payloadCaptor.getValue().priority());
    }

    @Test
    void place_rejectsBlankRegion() {
        var cmd = new PlaceOrderCommand(
                UUID.randomUUID(),
                "  ",
                OrderPriority.LOW,
                new BigDecimal("10.00"),
                List.of(line())
        );

        assertThrows(IllegalArgumentException.class, () -> useCase.place(cmd));
        verifyNoInteractions(persistencePort, outboxStorePort);
    }

    @Test
    void place_rejectsEmptyLines() {
        var cmd = new PlaceOrderCommand(
                UUID.randomUUID(),
                "EU",
                OrderPriority.NORMAL,
                new BigDecimal("10.00"),
                List.of()
        );

        assertThrows(IllegalArgumentException.class, () -> useCase.place(cmd));
        verifyNoInteractions(persistencePort, outboxStorePort);
    }

    private static OrderLine line() {
        OrderLine line = new OrderLine();
        line.setProductId(UUID.randomUUID());
        line.setQuantity(1);
        line.setPrice(new BigDecimal("10.00"));
        return line;
    }
}