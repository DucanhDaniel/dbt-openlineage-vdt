# Kịch bản Demo: dbt + Airflow + OpenLineage

Kịch bản này bao gồm 3 tình huống (scenarios) để trình diễn cách hệ thống phản hồi và ghi nhận data lineage thông qua OpenLineage khi pipeline được thực thi:

1. **Tình huống 1:** Thành công toàn bộ (Happy Path).
2. **Tình huống 2:** Lỗi thực thi Model (Model execution failure).
3. **Tình huống 3:** Lỗi kiểm thử dữ liệu (dbt test failure).

---

## 1. Tình huống 1: Pipeline chạy thành công (Happy Path)

**Mục tiêu:** Chứng minh toàn bộ pipeline dbt có thể chạy trơn tru từ đầu đến cuối và OpenLineage ghi nhận được đầy đủ đồ thị dữ liệu (data graph).

**Các bước thực hiện trong buổi Demo:**
1. **Chuẩn bị:** Đảm bảo mã nguồn dbt ở trạng thái gốc, không có lỗi. Code trong các model như `dim_ads`, `fact_ads_location`... đều đúng logic.
2. **Kích hoạt DAG:** Truy cập vào giao diện Airflow UI. Bật (Unpause) và Trigger DAG `dbt_transformation_dag`.
3. **Theo dõi tiến trình:** Chờ các task trong Airflow (dbt run, dbt test...) chạy qua và chuyển sang màu xanh lá (Success).
4. **Kiểm tra OpenLineage (Marquez UI):**
   - Truy cập giao diện Marquez (hoặc UI của hệ thống OpenLineage backend mà bạn đang dùng).
   - Tìm kiếm các job liên quan (VD: `dbt_openlineage.marts.dim_ads`).
   - Chỉ cho người xem đồ thị lineage được kết nối đầy đủ từ các bảng `staging` sang bảng `marts`, và nhấn mạnh rằng toàn bộ metadata thời gian chạy, cấu trúc bảng đều đã được ghi nhận.

---

## 2. Tình huống 2: Model bị lỗi (Execution Error)

**Mục tiêu:** Cho thấy hệ thống xử lý ra sao khi một model dbt bị lỗi (ví dụ: lỗi cú pháp SQL) và cách lỗi này hiển thị trên OpenLineage.

**Các bước thực hiện trong buổi Demo:**
1. **Tạo lỗi có chủ đích:** Mở file model `dbt_openlineage/models/marts/dim_ads.sql` và thêm một lỗi cú pháp hoặc tính toán sai vào câu lệnh `SELECT`.
   *Ví dụ (lỗi chia cho 0):*
   ```sql
   SELECT
      ad_id,
      ad_name,
      1 / 0 AS intentional_error_column -- Dòng thêm vào để gây lỗi
   FROM ...
   ```
2. **Kích hoạt DAG:** Trigger lại DAG `dbt_transformation_dag` trên Airflow.
3. **Theo dõi tiến trình (Airflow):**
   - Task chạy `dim_ads` sẽ bị báo lỗi màu đỏ (Failed).
   - Các task phụ thuộc (downstream models nếu có) sẽ bị đánh dấu là Skipped (màu cam).
4. **Kiểm tra OpenLineage (Marquez UI):**
   - Mở Marquez UI, tìm job tương ứng của model `dim_ads`.
   - Trạng thái của lần chạy gần nhất (Run) sẽ được đánh dấu là **FAILED**.
   - (Tuỳ thuộc vào OpenLineage backend) Có thể xem được Error Message trong phần chi tiết của Run để biết chính xác lý do lỗi.
5. **Khắc phục sau Demo:** Xóa dòng lỗi vừa thêm vào `dim_ads.sql` để trả lại trạng thái bình thường.

---

## 3. Tình huống 3: dbt test failed (Data Quality Issue)

**Mục tiêu:** Chứng minh rằng hệ thống có khả năng kiểm soát chất lượng dữ liệu. Khi dữ liệu không thỏa mãn điều kiện (dbt test fail), pipeline báo lỗi và OpenLineage cập nhật thông tin kiểm thử.

**Các bước thực hiện trong buổi Demo:**
1. **Tạo test thất bại có chủ đích:** Mở file cấu hình `dbt_openlineage/models/marts/_models.yml`. Cố tình thêm một test vô lý (như `accepted_values` sai) cho cột `ad_id` của model `dim_ads`.
   *Ví dụ:*
   ```yaml
   models:
     - name: dim_ads
       description: "Dimension table for ads"
       columns:
         - name: ad_id
           tests:
             - accepted_values:
                 values: ['Gia_tri_nay_khong_ton_tai'] # Test này chắc chắn sẽ Fail
   ```
2. **Kích hoạt DAG:** Trigger lại DAG `dbt_transformation_dag` trên Airflow.
3. **Theo dõi tiến trình (Airflow):**
   - Task `dbt run` (biên dịch và chạy model `dim_ads`) sẽ báo màu xanh (Success).
   - Nhưng task `dbt test` cho model `dim_ads` sẽ bị báo lỗi màu đỏ (Failed).
4. **Kiểm tra OpenLineage (Marquez UI):**
   - Trên giao diện Marquez, kiểm tra dataset hoặc job `dim_ads`.
   - Tìm đến tab **Data Quality** (hoặc phần Facets của job đó).
   - Tại đây, hệ thống sẽ log lại thông tin một data quality test tên là `accepted_values_dim_ads_ad_id...` đã bị **FAILED**, cho thấy sự kết nối liền mạch về tình trạng dữ liệu giữa dbt và OpenLineage.
5. **Khắc phục sau Demo:** Xóa đi dòng config test cố tình làm sai trong `_models.yml`.
