# Caro 5 — HTML client + Render realtime backend

## Tệp cần dùng

- `index.html`: toàn bộ giao diện và logic Caro phía trình duyệt trong một file. Adapter tương thích một phần với các API Firebase cũ được nhúng trực tiếp, nhưng dữ liệu realtime chạy qua Socket.IO đến Render. Không cần tải Firebase SDK.
- `server.js`: backend Node.js + Express + Socket.IO, có `GET /` và `GET /healthz` để kiểm tra sức khỏe dịch vụ.
- `package.json`: cấu hình chạy backend trên Render.

## Triển khai backend trên Render

Đưa `server.js` và `package.json` lên repository được Render Web Service dùng. Đặt:

- Build Command: `npm install`
- Start Command: `npm start`
- Health Check Path: `/healthz`

Sau deploy, kiểm tra `https://a-sl8r.onrender.com/healthz`. Mong đợi HTTP 200 với JSON `{"status":"ok"}`.

## Đưa HTML lên GitHub Pages

Tải `index.html` lên repository website (ví dụ đổi tên thành `index.html` nếu cần). HTML đã trỏ Socket.IO tới `https://a-sl8r.onrender.com`; không phải dán thêm URL thủ công trong mã.

## Lưu tài khoản và phiên đăng nhập bền vững

Backend tự tạo bảng PostgreSQL `caro_state` và lưu tài khoản, hồ sơ/ELO, lịch sử, trạng thái khóa, trận đấu cùng phiên đăng nhập. Khi máy chủ khởi động lại, dữ liệu sẽ được nạp lại từ cơ sở dữ liệu.

### Thiết lập cơ sở dữ liệu miễn phí bằng Neon

1. Tạo một project PostgreSQL tại https://neon.tech/ rồi sao chép connection string của database.
2. Mở Render Dashboard → service `a-sl8r` → **Environment**.
3. Thêm biến `DATABASE_URL`, dán toàn bộ connection string vào giá trị rồi lưu. Giữ connection string bí mật; không đưa vào `index.html` hoặc GitHub.
4. Đợi Render deploy xong rồi mở `https://a-sl8r.onrender.com/healthz`. Cần thấy `"persistentStorage": true`; ở `https://a-sl8r.onrender.com/`, trường `storage` cần là `"postgres"`.

Nếu chưa cấu hình `DATABASE_URL`, backend vẫn chạy ở chế độ RAM để không làm hỏng trang, nhưng tài khoản và phiên đăng nhập vẫn có thể mất khi restart. Không coi là đã lưu bền vững cho tới khi kiểm tra được `persistentStorage: true`.

## Giới hạn cần biết

- HTML không thể làm cho backend đang lỗi tự hoạt động. Nếu `/healthz` không trả HTTP 200, hãy sửa/deploy backend trước.
- Khi dùng PostgreSQL, backend lưu dữ liệu thành một snapshot trong một bảng và được thiết kế để chạy một instance backend.
- Dữ liệu đã bị xóa khỏi RAM ở các lần khởi động trước không thể tự khôi phục từ bản sửa này; các tài khoản đó cần đăng ký lại.
- Đây là adapter tương thích một phần để giữ cấu trúc logic trong HTML hiện có; nên kiểm thử ghép trận từ hai trình duyệt/thiết bị trước khi công bố.
