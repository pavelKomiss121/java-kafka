package ru.mentee.power.orders.ports.incoming;

/**
 * Входящий порт: «кому-то снаружи (планировщику) нужно попросить домен
 * опубликовать очередную пачку накопившихся outbox-событий».
 */
public interface DispatchOutboxPort {

    DispatchResult dispatchDueBatch(int batchSize);

    record DispatchResult(int sent, int failed, int dead) {}
}