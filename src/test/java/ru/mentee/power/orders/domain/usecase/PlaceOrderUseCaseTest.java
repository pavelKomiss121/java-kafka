package ru.mentee.power.orders.domain.usecase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort.PlaceOrderCommand;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PlaceOrderUseCaseTest {

    @Mock
    OrderEventPort orderEventPort;

    @InjectMocks
    PlaceOrderUseCase useCase;

    @Test
    void place_publishesEvent_whenValid() {
        when(orderEventPort.publish(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        var cmd = new PlaceOrderCommand(
                UUID.randomUUID(),
                "EU",
                OrderPriority.HIGH,
                new BigDecimal("10.00"),
                List.of(line())
        );

        var result = useCase.place(cmd);

        assertEquals("QUEUED", result.status());
        verify(orderEventPort).publish(any(), eq(OrderPriority.HIGH));
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
        verifyNoInteractions(orderEventPort);
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
        verifyNoInteractions(orderEventPort);
    }

    private static OrderLine line() {
        OrderLine line = new OrderLine();
        line.setProductId(UUID.randomUUID());
        line.setQuantity(1);
        line.setPrice(new BigDecimal("10.00"));
        return line;
    }
}
