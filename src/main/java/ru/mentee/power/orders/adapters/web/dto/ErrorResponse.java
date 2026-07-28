package ru.mentee.power.orders.adapters.web.dto;

/** Ошибки 400 / 500. */
public class ErrorResponse {

    private String code;
    private String message;

    public ErrorResponse() {
    }

    public ErrorResponse(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
