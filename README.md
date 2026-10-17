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

## Giới hạn cần biết

- HTML không thể làm cho backend đang lỗi tự hoạt động. Nếu `/healthz` không trả HTTP 200, hãy sửa/deploy backend trước.
- Backend mẫu lưu người chơi, trận đấu và bảng xếp hạng trong RAM. Dữ liệu bị mất khi Render restart hoặc redeploy; muốn lưu bền vững cần cơ sở dữ liệu.
- Backend mẫu chưa phải hệ thống xếp hạng công khai an toàn: trước khi mở cho nhiều người chơi, cần kiểm tra nước đi và điểm ELO phía máy chủ, xác thực người chơi, phân quyền từng ván và chống gian lận.
- Đây là adapter tương thích một phần để giữ cấu trúc logic trong HTML hiện có; nên kiểm thử ghép trận từ hai trình duyệt/thiết bị trước khi công bố.
