# HƯỚNG DẪN KẾT NỐI DATAGRIP ĐẾN TRINO

## Bước 1: Lấy JWT Token

Mở PowerShell/Terminal và chạy:

```powershell
cd D:\Lakehouse
python trino/datagrip-quick-connect.py
```

Bạn sẽ thấy output như:

```
================================================================================
COPY THIS URL TO DATAGRIP
================================================================================

jdbc:trino://localhost:443?accessToken=eyJhbGciOiJSUzI1NiIsInR5cC...

================================================================================
```

**Copy toàn bộ URL này** (từ `jdbc:trino` đến hết)

---

## Bước 2: Cấu hình DataGrip

### **General Tab:**

1. **Name:** `@localhost` (hoặc tên bạn muốn)

2. **Connection type:** `default`

3. **Driver:** `Trino`

4. **Host:** `localhost`

5. **Port:** `443`

6. **Authentication:**
   - Chọn **"No auth"** từ dropdown
   - ❌ KHÔNG chọn "User & Password"
7. **Database:**

   - Để trống HOẶC
   - Ghi `lake_bronze` (nếu muốn default catalog)

8. **URL:**

   - **Paste URL vừa copy** từ script vào đây
   - Ví dụ: `jdbc:trino://localhost:443?accessToken=eyJhbG...`

9. **Important:** Check dòng "Overrides settings above" xuất hiện

---

### **SSH/SSL Tab:**

1. **Use SSH tunnel:** ☐ UNCHECK (không dùng SSH tunnel)

2. **Use SSL:** ✅ **CHECK** (bắt buộc!)

3. **CA file:**

   - Để trống (empty)
   - Hoặc browse to: `D:\Lakehouse\certs\nginx.crt` (nếu muốn verify)

4. **Use truststore:**

   - ✅ CHECK: IDE
   - ✅ CHECK: Java
   - ✅ CHECK: System
   - (Check cả 3 options)

5. **Client certificate file:** Để trống

6. **Client key file:** Để trống

7. **Client key password:** Để trống

8. **Mode:**
   - Chọn **"Require"** từ dropdown
   - Các options:
     - `Disable` - Không dùng SSL (❌ không work)
     - `Allow` - Prefer plaintext
     - `Prefer` - Prefer SSL
     - `Require` ← **CHỌN CÁI NÀY**
     - `Verify CA` - Verify certificate (cần CA file)

---

### **Options Tab (Optional):**

Có thể bỏ qua tab này nếu đã dùng URL với accessToken.

---

### **Advanced Tab (Optional):**

Có thể bỏ qua tab này.

---

## Bước 3: Test Connection

1. Click nút **"Test Connection"** ở bottom left

2. **Expected result:**

   ```
   ✅ Connection successful
   Ping: 20-50 ms
   ```

3. **Nếu lỗi "Unauthorized":**

   - Token đã hết hạn (60 minutes)
   - Chạy lại `python trino/datagrip-quick-connect.py`
   - Copy URL mới và replace trong DataGrip

4. **Nếu lỗi "Access Denied":**

   - User chưa có permissions trong RBAC
   - Chạy:
     ```powershell
     docker exec rbac-db psql -U rbac -d rbac -c "INSERT INTO policy (username, catalog, schema_name, table_name, columns, actions) VALUES ('hung', NULL, NULL, NULL, NULL, '{ALL}') ON CONFLICT DO NOTHING;"
     ```

5. **Nếu lỗi "SSL":**
   - Check "Use SSL" đã checked
   - Check Mode = "Require"
   - Check Port = 443 (không phải 8080)

---

## Bước 4: Sử dụng

Sau khi kết nối thành công:

1. **Xem catalogs:**

   ```sql
   SHOW CATALOGS;
   ```

2. **Xem schemas:**

   ```sql
   SHOW SCHEMAS FROM lake_bronze;
   ```

3. **Query dữ liệu:**
   ```sql
   SELECT * FROM lake_bronze.bronze.your_table LIMIT 10;
   ```

---

## ⏱️ Token Expiration

Token hết hạn sau **60 phút**.

**Khi token hết hạn:**

1. Chạy lại script:

   ```powershell
   python trino/datagrip-quick-connect.py
   ```

2. Copy URL mới

3. Trong DataGrip:

   - Right click connection → Properties
   - General Tab → URL field
   - Replace URL cũ bằng URL mới
   - Click "OK"

4. **Không cần restart DataGrip**

---

## 🎯 Tại sao KHÔNG thể dùng User & Password trực tiếp?

### Bạn có thể thắc mắc:

```
❓ Tại sao không thể như này?

┌────────────────────────────────────┐
│ Authentication: User & Password    │
│ User:     hung                     │
│ Password: hung                     │
└────────────────────────────────────┘
```

### Câu trả lời:

**Trino JDBC Driver KHÔNG hỗ trợ OAuth2 Password Grant!**

**Flow hiện tại:**

```
User → Script → Keycloak (username/password) → JWT Token → DataGrip → Trino
```

**Flow mong muốn (KHÔNG khả thi):**

```
DataGrip (username/password) → ❌ JDBC Driver không biết call Keycloak
```

**Lý do kỹ thuật:**

1. JDBC Driver chỉ biết:

   - Gửi username/password qua Basic Auth
   - Gửi JWT token qua Bearer Auth
   - **KHÔNG biết** call OAuth2 token endpoint

2. Trino Server yêu cầu:

   - JWT token trong Authorization header
   - **KHÔNG chấp nhận** username/password

3. JDBC Standard:
   - Không có OAuth2 support
   - Chỉ có: Basic, Digest, Kerberos

---

## 🛠️ Giải pháp thay thế

### Option 1: Script Helper (Đang dùng) ⭐ Recommended

**Pros:**

- ✅ Đơn giản, dễ dùng
- ✅ Vẫn dùng OAuth2/Keycloak
- ✅ Centralized authentication

**Cons:**

- ⚠️ Cần refresh token mỗi 60 phút
- ⚠️ Phải run script

### Option 2: Tăng token lifetime

Tăng thời gian sống của token lên 8 giờ:

1. Login Keycloak Admin: http://localhost:30080/admin

   - Username: `admin`
   - Password: `admin`

2. Realm: `iceberg` → Realm Settings → Tokens

3. **Access Token Lifespan:**

   - 1 Minutes → **480 Minutes** (8 hours)

4. Click **Save**

5. Giờ token sẽ valid 8 giờ thay vì 1 giờ

### Option 3: Reconfigure Trino với Password Auth

**Thay OAuth2 bằng File-based Password Authentication:**

**Pros:**

- ✅ Nhập username/password trực tiếp trong DataGrip
- ✅ Không cần token

**Cons:**

- ❌ Mất OAuth2/Keycloak integration
- ❌ Phải quản lý users riêng biệt
- ❌ Mất centralized authentication
- ❌ Phức tạp hơn

**Nếu muốn thử, tôi có thể hướng dẫn chi tiết.**

---

## 📸 Screenshots Tham khảo

### General Tab - Cấu hình đúng:

```
┌─────────────────────────────────────────────────┐
│ Name: @localhost                                │
│                                                 │
│ Host: localhost          Port: 443              │
│                                                 │
│ Authentication: [No auth ▼]                     │
│                                                 │
│ Database: lake_bronze                           │
│                                                 │
│ URL: jdbc:trino://localhost:443?accessToken=... │
│      ↑ Overrides settings above                 │
└─────────────────────────────────────────────────┘
```

### SSH/SSL Tab - Cấu hình đúng:

```
┌─────────────────────────────────────────────────┐
│ ☐ Use SSH tunnel                                │
│                                                 │
│ ✅ Use SSL                                      │
│                                                 │
│    CA file: (empty)                             │
│                                                 │
│    Use truststore:                              │
│    ☑ IDE   ☑ Java   ☑ System                   │
│                                                 │
│    Mode: [Require ▼]                            │
└─────────────────────────────────────────────────┘
```

---

## ✅ Checklist

Trước khi test connection, check:

- [ ] Python script đã chạy và copy được URL
- [ ] URL đã paste vào DataGrip URL field
- [ ] Authentication = "No auth" (KHÔNG phải "User & Password")
- [ ] SSH/SSL tab: "Use SSL" = CHECKED
- [ ] SSH/SSL tab: Mode = "Require"
- [ ] Port = 443 (không phải 8080)
- [ ] Services đang chạy (trino-opa, trino-proxy, keycloak)

---

## 🐛 Troubleshooting

### Lỗi: "Connection refused"

**Nguyên nhân:** Services chưa chạy

**Fix:**

```powershell
docker ps | Select-String "trino"
# Nếu không thấy trino-opa và trino-proxy:
docker compose -f docker-compose-trino.yaml up -d
```

### Lỗi: "Unauthorized"

**Nguyên nhân:** Token hết hạn

**Fix:** Chạy lại `python trino/datagrip-quick-connect.py` và update URL

### Lỗi: "Access Denied"

**Nguyên nhân:** User chưa có permissions

**Fix:**

```powershell
docker exec rbac-db psql -U rbac -d rbac -c "SELECT * FROM policy WHERE username='hung';"
# Nếu empty:
docker exec rbac-db psql -U rbac -d rbac -c "INSERT INTO policy (username, catalog, schema_name, table_name, columns, actions) VALUES ('hung', NULL, NULL, NULL, NULL, '{ALL}');"
```

### Lỗi: "SSL peer shut down incorrectly"

**Nguyên nhân:** SSL mode không đúng

**Fix:**

- Check "Use SSL" = CHECKED
- Change Mode to "Require"
- Uncheck "Use SSH tunnel"
