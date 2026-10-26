package dev.souradip.relay.tenant;

import dev.souradip.relay.RelayProperties;
import dev.souradip.relay.api.ApiException;
import jakarta.validation.Valid;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
class TenantController {

    private final TenantService tenants;
    private final byte[] adminToken;

    TenantController(TenantService tenants, RelayProperties properties) {
        this.tenants = tenants;
        this.adminToken = properties.adminToken().getBytes(StandardCharsets.UTF_8);
    }

    @PostMapping("/admin/tenants")
    @ResponseStatus(HttpStatus.CREATED)
    CreatedTenant create(@RequestHeader("X-Admin-Token") String token, @Valid @RequestBody CreateTenantRequest request) {
        if (!MessageDigest.isEqual(adminToken, token.getBytes(StandardCharsets.UTF_8))) {
            throw new ApiException(HttpStatus.FORBIDDEN, "FORBIDDEN", "admin token is invalid");
        }
        return tenants.create(request);
    }
}
