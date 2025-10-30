"""
Test script for numbered document recognition feature
"""
import re
from typing import Optional

def extract_numbered_title(title: str, content: str = "") -> Optional[tuple]:
    """
    Extract numbered document information from title or content.
    Returns (number, full_title) or None.
    
    Example:
        "01. Quyết định về việc ban hành..." -> ("01", "Quyết định về việc ban hành...")
        "02. Quy định về đánh giá..." -> ("02", "Quy định về đánh giá...")
    
    Note: Keeps the full Vietnamese title with original formatting.
    """
    # Try to find pattern like "01.", "02.", etc. at the beginning
    pattern = r'^(\d{1,3})[\.\)]\s*(.+?)(?:\s*$)'
    
    # Search in title first
    match = re.search(pattern, title.strip(), re.MULTILINE)
    if not match:
        # Try to find in content (first few lines)
        first_lines = content[:1000] if content else ""
        match = re.search(pattern, first_lines, re.MULTILINE)
    
    if match:
        number = match.group(1).zfill(2)  # Pad to 2 digits: "1" -> "01"
        doc_title = match.group(2).strip()
        
        # Clean up excessive whitespace but keep Vietnamese characters
        doc_title = re.sub(r'\s+', ' ', doc_title)
        # Remove any trailing punctuation
        doc_title = doc_title.rstrip('.,;:')
        # Limit length
        doc_title = doc_title[:200]
        
        return (number, doc_title)
    
    return None

def get_nhom_lon(url: str, title: str) -> str:
    """
    Determine the main category group (nhom_lon) for quydinh_huongdan.
    
    Categories:
    - qui-che-qui-dinh-qui-trinh: Official regulations and procedures
    - huong-dan-sinh-vien: Student guidance documents
    - bieumau-bangbieucuahang: Forms and templates
    
    Returns the category slug or default.
    """
    text = (url + " " + title).lower()
    
    # Check for forms/templates
    if any(x in text for x in ["biểu mẫu", "bieu-mau", "bảng biểu", "bang-bieu", "mẫu đơn"]):
        return "bieumau-bangbieucuahang"
    
    # Check for student guidance
    if any(x in text for x in ["hướng dẫn sinh viên", "huong-dan-sinh-vien", 
                                "đăng ký", "dang-ky", "tốt nghiệp", "tot-nghiep",
                                "học vụ", "hoc-vu"]):
        return "huong-dan-sinh-vien"
    
    # Default to qui-che-qui-dinh-qui-trinh (most common)
    return "qui-che-qui-dinh-qui-trinh"

# Test cases with URLs to determine nhom_lon
test_cases = [
    ("01. Quyết định về việc ban hành Quy chế đào tạo theo học chế tín chỉ", "https://daa.uit.edu.vn/qui-che-qui-dinh-qui-trinh"),
    ("02. Quy định về đánh giá kết quả học tập", "https://daa.uit.edu.vn/qui-che-qui-dinh-qui-trinh"),
    ("03. Quy trình đăng ký học phần", "https://daa.uit.edu.vn/huong-dan-sinh-vien/dang-ky"),
    ("1) Hướng dẫn thực hiện công tác đào tạo", "https://daa.uit.edu.vn/huong-dan"),
    ("10. Biểu mẫu xin cấp bảng điểm", "https://daa.uit.edu.vn/bieu-mau"),
    ("Quy định không có số", "https://daa.uit.edu.vn/qui-che"),  # Should return None
]

print("=" * 80)
print("TESTING NUMBERED DOCUMENT RECOGNITION - CẤU TRÚC MỚI")
print("=" * 80)

for i, (title, url) in enumerate(test_cases, 1):
    result = extract_numbered_title(title)
    print(f"\n{i}. Title: {title}")
    print(f"   URL: {url}")
    if result:
        number, doc_title = result
        nhom_lon = get_nhom_lon(url, title)
        numbered_folder = f"{number}. {doc_title}"
        folder = f"daa/quydinh_huongdan/{nhom_lon}/{numbered_folder}"
        print(f"   ✓ Detected: {numbered_folder}")
        print(f"   ✓ Nhóm lớn: {nhom_lon}")
        print(f"   → Full path: {folder}")
    else:
        nhom_lon = get_nhom_lon(url, title)
        print(f"   ✗ No numbered pattern detected")
        print(f"   → Will use: daa/quydinh_huongdan/{nhom_lon}/")

print("\n" + "=" * 80)
print("EXAMPLE FILE STRUCTURE - CẤU TRÚC CHUẨN MỚI:")
print("=" * 80)
print("""
data/
└── daa/
    └── quydinh_huongdan/
        ├── qui-che-qui-dinh-qui-trinh/
        │   ├── 01. Quyết định về việc ban hành Quy chế đào tạo theo học chế tín chỉ/
        │   │   ├── 790-qd-dhcntt_28-9-22_quy_che_dao_tao.pdf
        │   │   ├── 792-qd-dhcntt_30-9-22_quy_che_dao_tao.pdf
        │   │   └── content.html
        │   └── 02. Quy định về đánh giá kết quả học tập/
        │       ├── qd_danh_gia_2023.pdf
        │       └── content.html
        ├── huong-dan-sinh-vien/
        │   └── 03. Quy trình đăng ký học phần/
        │       ├── huong_dan_dang_ky.pdf
        │       └── content.html
        └── bieumau-bangbieucuahang/
            └── 10. Biểu mẫu xin cấp bảng điểm/
                └── mau_xin_cap_bang_diem.pdf

Lưu ý: Files được lưu TRỰC TIẾP trong thư mục số (không có subfolder pdf/html/markdown)
""")

print("\n" + "=" * 80)
print("CRAWLED.TXT FORMAT - ĐỊNH DẠNG CHUẨN:")
print("=" * 80)
print("""
daa/quydinh_huongdan/qui-che-qui-dinh-qui-trinh/01. Quyết định về việc ban hành Quy chế đào tạo theo học chế tín chỉ/790-qd-dhcntt_28-9-22_quy_che_dao_tao.pdf - url: https://daa.uit.edu.vn/sites/daa/files/790-qd-dhcntt_28-9-22_quy_che_dao_tao.pdf
daa/quydinh_huongdan/qui-che-qui-dinh-qui-trinh/01. Quyết định về việc ban hành Quy chế đào tạo theo học chế tín chỉ/content.html - url: https://daa.uit.edu.vn/qui-che-qui-dinh-qui-trinh/1234
daa/quydinh_huongdan/huong-dan-sinh-vien/03. Quy trình đăng ký học phần/huong_dan_dang_ky.pdf - url: https://daa.uit.edu.vn/sites/daa/files/huong_dan_dang_ky.pdf

Lưu ý: Đường dẫn bao gồm đầy đủ:
- Nhóm lớn (qui-che-qui-dinh-qui-trinh, huong-dan-sinh-vien, hoặc bieumau-bangbieucuahang)
- Thư mục số có tên đầy đủ bằng tiếng Việt (giữ nguyên dấu, khoảng trắng)
- Tên file gốc từ URL
""")
