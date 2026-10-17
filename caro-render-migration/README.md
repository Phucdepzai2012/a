# Caro: chuyển lớp kết nối từ Firebase sang Render + Socket.IO

Bộ này giữ nguyên logic bàn cờ/game của HTML hiện tại bằng cách cung cấp adapter tương thích một phần với các lệnh Firebase Realtime Database mà code Caro đang gọi. Máy chủ sử dụng Socket.IO trên `https://a-sl8r.onrender.com`.

## 1. Thay hai phần trong `index.html`

**Xóa ba thẻ Firebase SDK** hiện có:

```html
<script src="https://www.gstatic.com/firebasejs/10.12.2/firebase-app-compat.js"></script>
<script src="https://www.gstatic.com/firebasejs/10.12.2/firebase-auth-compat.js"></script>
<script src="https://www.gstatic.com/firebasejs/10.12.2/firebase-database-compat.js"></script>
```

Thay bằng hai thẻ sau, đặt ngay trước thẻ `<script>` lớn chứa code Caro:

```html
<script src="https://cdn.socket.io/4.8.1/socket.io.min.js"></script>
<script src="./render-firebase-adapter.js"></script>
```

Đặt `render-firebase-adapter.js` cùng thư mục với `index.html`. Giữ nguyên script game còn lại. Các hằng cấu hình Firebase cũ trong script sẽ không còn được dùng để kết nối Firebase; có thể dọn chúng sau khi đã kiểm tra game hoạt động.

## 2. Đưa backend lên Render

Đưa `server.js` và `package.json` trong thư mục này vào repository GitHub đang liên kết với Render service `a-sl8r` (nếu repository có file backend hiện tại, hãy sao lưu trước khi thay). Trong Render, đặt:

- **Build Command:** `npm install`
- **Start Command:** `npm start`
- **Health Check Path:** `/healthz`

Sau khi deploy thành công, `https://a-sl8r.onrender.com/` phải trả JSON tương tự `{"service":"caro-realtime","status":"ok"}` và `/healthz` trả `{"status":"ok"}`.

## 3. Giới hạn quan trọng

- Khi kiểm tra, URL `https://a-sl8r.onrender.com` đang trả **503 Service Unavailable**. Nghĩa là máy chủ hiện chưa sẵn sàng; adapter sẽ không thể kết nối đến khi service deploy/chạy thành công.
- Đây là backend mẫu tối giản để chuyển giao thức. Dữ liệu hiện ở RAM và **mất khi Render khởi động lại/deploy**; bảng xếp hạng và hồ sơ không bền vững. Muốn lưu lâu dài cần gắn database bền vững và tích hợp vào server.
- Bộ mẫu không có xác thực/phân quyền production-grade. Người dùng có thể giả mạo dữ liệu hồ sơ/điểm nếu public nguyên trạng. Trước khi mở rộng cho nhiều người chơi, cần thêm xác thực, kiểm tra quyền từng ván và xác minh nước đi/điểm số ở server.
- Đây là adapter tương thích với các thao tác đang có trong code đã gửi, không phải bản triển khai Firebase hoàn chỉnh.
