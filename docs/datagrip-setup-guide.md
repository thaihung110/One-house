# DataGrip Connection Guide - Trino with OAuth2/JWT

## Cấu hình DataGrip kết nối Trino

### Bước 1: Lấy JWT Token

Mở terminal và chạy:

```bash
python D:\Lakehouse\trino\get-token.py
```

Script sẽ output token và hướng dẫn cấu hình.

---

## ✅ Cách 1: URL với accessToken (Recommended - Đơn giản nhất)

### **General Tab:**

```
Name: @localhost
Host: localhost
Port: 443
```

**URL:**

```
jdbc:trino://localhost:443?accessToken=<PASTE_TOKEN_HERE>
```

**Authentication:**

- Type: `No auth`

### **SSH/SSL Tab:**

✅ **Use SSL: CHECKED**

**Mode:** `Require` (no verification)

**Giải thích:**

- Mode "Require" chấp nhận self-signed certificates
- Không cần CA file vì đang dùng self-signed cert

**Screenshot cấu hình:**

```
┌─────────────────────────────────────┐
│ ✅ Use SSL                          │
│                                     │
│ Mode: [Require ▼]                   │
│                                     │
│ CA file: (empty)                    │
│                                     │
│ Use truststore:                     │
│ ☑ IDE  ☑ Java  ☑ System            │
└─────────────────────────────────────┘
```

### **Test Connection:**

Click "Test Connection" → Should show: ✅ **Succeeded**

---

## ⚙️ Cách 2: Properties Tab (Alternative)

Nếu URL quá dài, bạn có thể dùng Properties:

### **General Tab:**

**URL:**

```
jdbc:trino://localhost:443
```

**Authentication:**

- Type: `No auth`

### **Options Tab → Properties:**

Click **[+]** để add properties:

| Name              | Value                |
| ----------------- | -------------------- |
| `SSL`             | `true`               |
| `SSLVerification` | `NONE`               |
| `accessToken`     | `<PASTE_TOKEN_HERE>` |

### **SSH/SSL Tab:**

✅ **Use SSL: CHECKED**

Mode: `Require`

---

## 🔄 Refresh Token (Khi token hết hạn)

Token expires sau **60 phút**. Khi hết hạn:

1. Chạy lại: `python D:\Lakehouse\trino\get-token.py`
2. Copy token mới
3. **Cách nhanh:** Edit URL trong DataGrip → Replace token cũ bằng token mới
4. **Không cần** restart DataGrip

---

## ❌ Tại sao KHÔNG thể dùng User & Password trực tiếp?

### Vấn đề:

```
┌────────────────────────────────────┐
│ Authentication: [User & Password ▼]│
│                                    │
│ User:     hung                     │
│ Password: hung                     │
└────────────────────────────────────┘
```

**❌ Cấu hình này KHÔNG work với OAuth2/JWT!**

### Lý do kỹ thuật:

1. **JDBC Driver không hỗ trợ OAuth2 Password Grant:**
   - Driver chỉ nhận JWT token đã sẵn có
   - Không có code để gọi Keycloak token endpoint
2. **Trino server yêu cầu JWT token:**

   - Server config: `http-server.authentication.type=jwt,oauth2`
   - Server expects: `Authorization: Bearer <token>` header
   - Username/password không được chấp nhận

3. **JDBC Standard không có OAuth2:**
   - JDBC spec chỉ có: Basic Auth, Kerberos
   - OAuth2 không phải part của JDBC standard

---

## 🛠️ Workaround: Auto-refresh Script

Tạo helper script để tự động refresh token:

### File: `D:\Lakehouse\trino\refresh-datagrip-token.py`

```python
#!/usr/bin/env python3
"""
Auto-refresh JWT token for DataGrip
Run this script, it will print the JDBC URL with fresh token
"""
import requests
import pyperclip  # pip install pyperclip

KEYCLOAK_TOKEN_URL = "http://localhost:30080/realms/iceberg/protocol/openid-connect/token"

response = requests.post(
    KEYCLOAK_TOKEN_URL,
    data={
        "grant_type": "password",
        "client_id": "trino",
        "client_secret": "AK48QgaKsqdEpP9PomRJw7l2T7qWGHdZ",
        "username": "hung",
        "password": "hung",
        "scope": "trino",
    },
)

token = response.json()["access_token"]
jdbc_url = f"jdbc:trino://localhost:443?accessToken={token}"

# Copy to clipboard
pyperclip.copy(jdbc_url)

print("✅ Token refreshed and copied to clipboard!")
print("\nJust paste into DataGrip URL field and click 'Test Connection'")
print(f"\nToken expires in: {response.json()['expires_in'] // 60} minutes")
```

**Usage:**

```bash
python D:\Lakehouse\trino\refresh-datagrip-token.py
```

Script sẽ:

1. Lấy token mới từ Keycloak (dùng username/password)
2. Copy JDBC URL vào clipboard
3. Bạn chỉ cần paste vào DataGrip URL field

---

## 🔐 SSL/TLS Configuration Details

### Option 1: No Verification (Development)

**SSH/SSL Tab:**

```
✅ Use SSL
Mode: Require (no verification)
```

**Pros:**

- ✅ Đơn giản
- ✅ Không cần import certificate
- ✅ Work với self-signed cert

**Cons:**

- ⚠️ Không verify server identity
- ⚠️ Vulnerable to MITM attacks
- ⚠️ Chỉ dùng cho dev/testing

### Option 2: Verify CA (Production)

**SSH/SSL Tab:**

```
✅ Use SSL
Mode: Verify CA
CA file: D:\Lakehouse\certs\nginx.crt
```

**Steps:**

1. Export certificate từ nginx:

   ```bash
   # Certificate đã có sẵn tại:
   D:\Lakehouse\certs\nginx.crt
   ```

2. Configure DataGrip:
   - Mode: `Verify CA`
   - CA file: Browse to `D:\Lakehouse\certs\nginx.crt`

**Pros:**

- ✅ Secure
- ✅ Verify server identity
- ✅ Production-ready

**Cons:**

- Phức tạp hơn
- Cần maintain certificate

### Option 3: Import to Java Keystore (Global)

Import certificate vào Java keystore để tất cả Java apps trust:

```bash
# Find Java home
java -XshowSettings:properties -version 2>&1 | findstr "java.home"

# Import certificate
keytool -import -alias trino-nginx -file D:\Lakehouse\certs\nginx.crt -keystore "%JAVA_HOME%\lib\security\cacerts" -storepass changeit
```

Sau đó trong DataGrip:

```
✅ Use SSL
Mode: Require
Use truststore: ☑ Java
```

---

## 📊 So sánh các phương pháp

| Phương pháp                           | Username/Password trực tiếp | Token refresh frequency | Độ phức tạp                             |
| ------------------------------------- | --------------------------- | ----------------------- | --------------------------------------- |
| **URL với token**                     | ❌ (dùng script)            | 60 min                  | ⭐ Dễ                                   |
| **Properties với token**              | ❌ (dùng script)            | 60 min                  | ⭐⭐ Trung bình                         |
| **Auto-refresh script**               | ✅ (gián tiếp)              | Run script when needed  | ⭐ Rất dễ                               |
| **Password Auth** (không dùng OAuth2) | ✅                          | Không cần               | ⭐⭐⭐ Phức tạp (cần reconfigure Trino) |

---

## 🎯 Khuyến nghị

**Cho Development:**

1. ✅ Dùng URL với accessToken
2. ✅ SSL Mode: Require (no verification)
3. ✅ Dùng `refresh-datagrip-token.py` để refresh nhanh

**Cho Production:**

1. ✅ Import certificate vào Java keystore
2. ✅ SSL Mode: Verify CA
3. ✅ Consider setup LDAP/Password authentication thay vì OAuth2

---

## ❓ FAQ

### Q: Tại sao không thể như MySQL/PostgreSQL (nhập username/password)?

**A:** MySQL/PostgreSQL dùng Basic Authentication (username/password trong connection). Trino setup của bạn dùng OAuth2/JWT - modern security model yêu cầu token-based authentication.

### Q: Có cách nào để username/password work không?

**A:** Có 2 cách:

1. **Reconfigure Trino với Password Authentication:**

   - Bỏ OAuth2, dùng file-based hoặc LDAP authentication
   - Mất centralized authentication với Keycloak
   - Mất tính bảo mật của OAuth2

2. **Tạo custom JDBC wrapper/proxy:**
   - Complex implementation
   - Wrapper nhận username/password → call Keycloak → inject token vào request
   - Không khuyến nghị

### Q: Token expires quá nhanh (60 min)?

**A:** Có thể tăng token lifetime trong Keycloak:

1. Login Keycloak Admin: http://localhost:30080/admin
2. Realm: iceberg → Realm Settings → Tokens
3. Access Token Lifespan: 60m → 480m (8 hours)
4. Click Save

---

## 🔧 Troubleshooting

### Lỗi: "SSL peer shut down incorrectly"

**Fix:** Thay đổi SSL Mode từ "Verify CA" → "Require"

### Lỗi: "Unauthorized"

**Fix:** Token đã hết hạn, chạy lại `get-token.py` để lấy token mới

### Lỗi: "Access Denied"

**Fix:** User chưa có permissions. Check RBAC:

```sql
-- Connect to rbac-db
docker exec rbac-db psql -U rbac -d rbac -c "SELECT * FROM policy WHERE username='hung';"
```

Nếu empty, add permissions:

```sql
docker exec rbac-db psql -U rbac -d rbac -c "INSERT INTO policy (username, catalog, schema_name, table_name, columns, actions) VALUES ('hung', NULL, NULL, NULL, NULL, '{ALL}');"
```
