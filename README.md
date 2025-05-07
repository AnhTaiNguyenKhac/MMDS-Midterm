# MMDS-Midterm

Cho tập tin baskets.csv chứa dữ liệu mua hàng của người dùng. Trong đó, dòng đầu tiên chứa
tiêu đề (header), các dòng còn lại là dữ liệu tương ứng.
- Member_number: mã số khách hàng
- Date: ngày mua hàng dạng dd/mm/yyyy
- itemDescription: tên của một món hàng
- year: năm mua
- month: tháng mua
- day: ngày mua
- day_of_week: thứ trong tuần

## 1) Câu 1: PySpark RDDs & DataFrames
### a) Sử dụng RDD (không sử dụng DataFrame) trong thư viện PySpark để đọc tập tin baskets.csv. Sau đó cài đặt, thực thi, lưu trữ và trực quan hoá kết quả các hàm sau.

| Hàm | Input                                   | Output                                    | Xử lý                                                                                                                |
|-----|-----------------------------------------|-------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| f1  | Đường dẫn đến tập tin `baskets.csv`     | In kết quả ra màn hình và lưu xuống folder `f1` | - Tìm danh sách món hàng phân biệt, số lần mỗi món được mua.Xếp giảm dần theo số lần mua.  Chọn 100 món được mua nhiều nhất.  Vẽ biểu đồ cột trực quan hóa số lần mua. |
| f2  | Đường dẫn đến tập tin `baskets.csv`     | In kết quả ra màn hình và lưu xuống folder `f2` | - Tìm số lượng giỏ hàng mỗi người đã mua.  Một giỏ hàng là tập hợp các món hàng người dùng mua trong một ngày. Xếp giảm dần theo số lượng giỏ hàng.  Chọn 100 người dùng mua nhiều giỏ hàng nhất.  Vẽ biểu đồ cột trực quan hóa. |
| f3  | Đường dẫn đến tập tin `baskets.csv`;  Tên món hàng quan tâm | In kết quả ra màn hình và lưu xuống folder `f3` | - Thống kê số lần món hàng được mua theo từng tháng.   Xếp tăng dần theo thời gian. Vẽ biểu đồ trực quan hóa theo thời gian. |
| f4  | Đường dẫn đến tập tin `baskets.csv`;  Mã khách hàng quan tâm | In kết quả ra màn hình và lưu xuống folder `f4` | - Thống kê số lần mua hàng của khách hàng theo từng tháng.   Xếp tăng dần theo thời gian. Vẽ biểu đồ trực quan hóa theo thời gian. |
### b) DataFrames: Thực hiện lại câu (a) trong đó sử dụng PySpark DataFrame
## 2) Câu 2: A-Priori Algorithm
Dữ liệu lưu trữ trên HDFS, viết chương trình Hadoop MapReduce (Java) để tìm ra các giỏ
hàng (tập hợp các món hàng được mua bởi 01 khách hàng trong 01 ngày, tháng, năm).

Cài đặt thuật toán A-Priori để tìm ra cặp phổ biến dưới dạng 02 chương trình Hadoop
MapReduce (Java), mỗi chương trình ứng với một pass.
## 3) Câu 3: PCY Algorithm
Dữ liệu lưu trữ trên Google Drive, sử dụng PySpark DataFrame để tìm ra các giỏ hàng
(tương tự câu 2).

Cài đặt thuật toán PCY để tìm cặp phổ biến và phát sinh các luật liên kết theo ngưỡng
support s và ngưỡng confidence c do người dùng truyền vào. Mô tả chi tiết và tường mình
hàm hash sử dụng, cách quản lý các bucket.

Thuật toán được tổ chức dạng lớp đối tượng để tiện lợi cho việc triển khai phần mềm về
sau. Tham khảo hình thức của lớp FPGrowth trong PySpark.
## 4) Câu 4: So sánh
Sử dụng basket tìm ra ở các câu trên.

Các chương trình trong câu này cài đặt dạng in-memory.

Cài đặt hai lớp đối tượng để thực thi thuật toán A-Priori và PCY.

Với kích thước dữ liệu tăng dần, so sánh thời gian thực thi để tìm cặp phổ biến của hai
thuật toán.

Trực quan hoá kết quả thí nghiệm để so sánh hiệu năng của hai thuật toán.
