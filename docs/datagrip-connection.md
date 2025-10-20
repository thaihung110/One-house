# Kết nối DataGrip với Trino qua OAuth2/JWT

## Vấn đề hiện tại

Bạn gặp lỗi **400 Bad Request** khi kết nối DataGrip với Trino vì:

1. **Authentication Type Mismatch**: Trino được cấu hình với OAuth2, nhưng DataGrip không native support OAuth2 password grant
2. **JWT Token trong URL**: JDBC URL với `accessToken` parameter không hoạt động vì Trino cần token trong Authorization header
3. **Nginx Buffer Size**: JWT tokens rất dài, cần tăng buffer size

## Giải pháp đã áp dụng

### 1. ✅ Cấu hình Trino hỗ trợ cả JWT và OAuth2

File: `trino/etc/config.properties`

```properties
# Support both JWT (for JDBC) and OAuth2 (for Web UI)
http-server.authentication.type=jwt,oauth2
# JWT Authentication for JDBC clients
http-server.authentication.jwt.key-file=http://localhost:30080/realms/iceberg/protocol/openid-connect/certs
http-server.authentication.jwt.principal-field=preferred_username
```

**Giải thích:**

- `jwt`: Cho phép JDBC clients gửi JWT token trong Authorization header
- `oauth2`: Giữ nguyên cho Web UI (browser-based authentication)

### 2. ✅ Cải thiện Nginx Configuration

File: `nginx.conf`

```nginx
http {
    # Increase buffer sizes for large JWT tokens
    large_client_header_buffers 4 32k;
    client_header_buffer_size 8k;
    proxy_buffer_size 16k;
    proxy_buffers 8 16k;
    proxy_busy_buffers_size 32k;

    server {
        listen 443 ssl;
        location / {
            proxy_pass http://trino-opa:8080;
            # Forward Authorization header for JWT tokens
            proxy_set_header Authorization $http_authorization;
            proxy_pass_header Authorization;
        }
    }
}
```

## Cách kết nối DataGrip

### ⚠️ Quan trọng: DataGrip Limitations

**DataGrip KHÔNG thể tự động lấy token từ Keycloak khi bạn nhập username/password.**

Lý do:

- Trino JDBC driver không có built-in OAuth2 password grant support
- DataGrip không native support OAuth2 flow
- JDBC standard không có OAuth2 authentication mechanism

### 🎯 Giải pháp khả thi: Sử dụng JWT Token

#### Bước 1: Lấy JWT Token

Chạy script Python:

```bash
cd trino
python trino-jdbc.py
```

Script sẽ:

1. Lấy token từ Keycloak bằng username=`hung`/password=`hung` (ROPC flow)
2. Hiển thị JWT token và hướng dẫn cấu hình

> 💡 Script tự động sử dụng credentials: username=`hung`, password=`hung`

#### Bước 2: Cấu hình DataGrip

**General Tab:**

- URL: `jdbc:trino://localhost:443`
- Driver: Trino

**Authentication:**

- Type: **User & Password**
- User: `hung`
- Password: `<paste JWT token here>`

> ⚠️ **Note:** Password field nhận JWT token, KHÔNG phải password của user `hung`

**SSH/SSL Tab:**

- ✅ Use SSL: **CHECKED**
- Mode: **Require** (no verification)
  - Hoặc **Verify CA** nếu bạn muốn verify certificate

**Options Tab (Optional):**
Có thể thêm custom properties:

- `SSL=true`
- `SSLVerification=NONE`

#### Bước 3: Test Connection

Click **Test Connection** trong DataGrip.

Nếu thành công, bạn sẽ thấy: ✅ **Connection successful**

### ⏱️ Token Expiration

**Quan trọng:**

- JWT token có thời gian sống ngắn (thường 60 phút)
- Khi token hết hạn, bạn cần:
  1. Chạy lại `python trino-jdbc.py`
  2. Copy token mới
  3. Update password trong DataGrip connection

## Giải pháp thay thế (Advanced)

### Option 1: Password File Authentication (Cần thêm setup)

Thêm vào `config.properties`:

```properties
http-server.authentication.type=password
http-server.authentication.password.user-mapping.pattern=(.*)
password-authenticator.name=file
file.password-file=/etc/trino/password.db
```

Tạo file password:

```bash
htpasswd -B -C 10 password.db hung
```

**Ưu điểm:** Nhập username/password trực tiếp trong DataGrip
**Nhược điểm:**

- Không tích hợp với Keycloak
- Quản lý users riêng biệt
- Mất centralized authentication

### Option 2: LDAP Authentication

Cấu hình Trino kết nối với LDAP/Active Directory:

```properties
http-server.authentication.type=password
http-server.authentication.password.user-mapping.pattern=(.*)
password-authenticator.name=ldap
ldap.url=ldap://ldap-server:389
```

**Ưu điểm:** Username/password authentication
**Nhược điểm:** Cần setup LDAP server

### Option 3: Custom JDBC Wrapper/Proxy

Tạo một proxy service:

1. DataGrip → Proxy (nhận username/password)
2. Proxy → Keycloak (lấy token)
3. Proxy → Trino (forward request với token)

**Ưu điểm:** Transparent cho user
**Nhược điểm:** Complex implementation

## Troubleshooting

### Lỗi: 400 Bad Request

**Nguyên nhân:**

1. Token chưa được forward đúng cách
2. Token đã hết hạn
3. URL format sai

**Giải pháp:**

```bash
# Check nginx logs
docker logs trino-proxy

# Check trino logs
docker logs trino-opa

# Restart services
docker compose -f docker-compose-trino.yaml restart trino-opa trino-proxy
```

### Lỗi: 401 Unauthorized

**Nguyên nhân:**

1. Token không hợp lệ
2. Token không có scope "trino"
3. Issuer không khớp

**Giải pháp:**

```bash
# Verify token có scope trino
python trino-jdbc.py
# Check trong JWT payload: "scope": "email profile trino"
```

### Lỗi: SSL Certificate

**Nguyên nhân:** Self-signed certificate

**Giải pháp:**

- Sử dụng `SSLVerification=NONE` trong connection
- Hoặc import certificate vào Java truststore

## Best Practices

### Development Environment

✅ **Khuyến nghị:**

- Sử dụng JWT token với script Python
- Token expiration = 60 phút (đủ cho dev session)
- SSLVerification=NONE cho self-signed certs

### Production Environment

✅ **Khuyến nghị:**

- Setup proper SSL certificates
- Sử dụng LDAP/Password authentication
- Hoặc implement custom OAuth2 proxy
- Enable SSL verification

## Tóm tắt

| Phương pháp             | Username/Password trực tiếp | Token expiration | Setup complexity   |
| ----------------------- | --------------------------- | ---------------- | ------------------ |
| **JWT Token (Current)** | ❌ (phải copy token)        | 60 min           | ⭐ Easy            |
| **Password File**       | ✅                          | N/A              | ⭐⭐ Medium        |
| **LDAP**                | ✅                          | N/A              | ⭐⭐⭐ Hard        |
| **Custom Proxy**        | ✅                          | Transparent      | ⭐⭐⭐⭐ Very Hard |

**Kết luận:** Với setup hiện tại, cách tốt nhất là sử dụng JWT token qua script Python. Nếu cần username/password trực tiếp, hãy xem xét Password File hoặc LDAP authentication.
