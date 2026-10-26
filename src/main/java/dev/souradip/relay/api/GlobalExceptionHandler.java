package dev.souradip.relay.api;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Errors are always JSON with the content type set explicitly. A chat client asks for
 * {@code text/event-stream}, and without the explicit type Spring would find no way to write an
 * error body in that format and answer 500 instead of the real status.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ApiException.class)
    ResponseEntity<ErrorBody> api(ApiException e) {
        var response = ResponseEntity.status(e.status());
        if (e.retryAfterSeconds() != null) {
            response.header("Retry-After", String.valueOf(e.retryAfterSeconds()));
        }
        return response.contentType(MediaType.APPLICATION_JSON).body(new ErrorBody(e.code(), e.getMessage()));
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    ResponseEntity<ErrorBody> missingHeader(MissingRequestHeaderException e) {
        return ResponseEntity.badRequest().contentType(MediaType.APPLICATION_JSON).body(new ErrorBody("MISSING_HEADER", e.getHeaderName() + " header is required"));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ErrorBody> invalid(MethodArgumentNotValidException e) {
        var first = e.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(f -> f.getField() + " " + f.getDefaultMessage())
                .orElse("request is invalid");
        return ResponseEntity.badRequest().contentType(MediaType.APPLICATION_JSON).body(new ErrorBody("INVALID_REQUEST", first));
    }
}
