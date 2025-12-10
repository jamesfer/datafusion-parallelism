#[inline]
pub fn const_inner_match_loop<'a, const N: u8, F, V>(search_hash: u64, base_index: usize, capacity_mask: usize, mut get_item: F) -> Option<&'a V>
where F: FnMut(usize) -> &'a (u64, V)
{
    for i in 0..8 {
        if N & (1 << i) != 0 {
            let item_index = (base_index + i) & capacity_mask;
            let item = get_item(item_index);
            if item.0 == search_hash {
                // Found a match
                return Some(&item.1);
            }
        }
    }

    None
}

#[inline]
pub fn dispatch_const_inner_match_loop<'a, F, V>(match_result: u8, search_hash: u64, base_index: usize, capacity_mask: usize, get_item: F) -> Option<&'a V>
where F: FnMut(usize) -> &'a (u64, V)
{
    match match_result {
        0 => const_inner_match_loop::<'a, 0, _, _>(search_hash, base_index, capacity_mask, get_item),
        1 => const_inner_match_loop::<'a, 1, _, _>(search_hash, base_index, capacity_mask, get_item),
        2 => const_inner_match_loop::<'a, 2, _, _>(search_hash, base_index, capacity_mask, get_item),
        3 => const_inner_match_loop::<'a, 3, _, _>(search_hash, base_index, capacity_mask, get_item),
        4 => const_inner_match_loop::<'a, 4, _, _>(search_hash, base_index, capacity_mask, get_item),
        5 => const_inner_match_loop::<'a, 5, _, _>(search_hash, base_index, capacity_mask, get_item),
        6 => const_inner_match_loop::<'a, 6, _, _>(search_hash, base_index, capacity_mask, get_item),
        7 => const_inner_match_loop::<'a, 7, _, _>(search_hash, base_index, capacity_mask, get_item),
        8 => const_inner_match_loop::<'a, 8, _, _>(search_hash, base_index, capacity_mask, get_item),
        9 => const_inner_match_loop::<'a, 9, _, _>(search_hash, base_index, capacity_mask, get_item),
        10 => const_inner_match_loop::<'a, 10, _, _>(search_hash, base_index, capacity_mask, get_item),
        11 => const_inner_match_loop::<'a, 11, _, _>(search_hash, base_index, capacity_mask, get_item),
        12 => const_inner_match_loop::<'a, 12, _, _>(search_hash, base_index, capacity_mask, get_item),
        13 => const_inner_match_loop::<'a, 13, _, _>(search_hash, base_index, capacity_mask, get_item),
        14 => const_inner_match_loop::<'a, 14, _, _>(search_hash, base_index, capacity_mask, get_item),
        15 => const_inner_match_loop::<'a, 15, _, _>(search_hash, base_index, capacity_mask, get_item),
        16 => const_inner_match_loop::<'a, 16, _, _>(search_hash, base_index, capacity_mask, get_item),
        17 => const_inner_match_loop::<'a, 17, _, _>(search_hash, base_index, capacity_mask, get_item),
        18 => const_inner_match_loop::<'a, 18, _, _>(search_hash, base_index, capacity_mask, get_item),
        19 => const_inner_match_loop::<'a, 19, _, _>(search_hash, base_index, capacity_mask, get_item),
        20 => const_inner_match_loop::<'a, 20, _, _>(search_hash, base_index, capacity_mask, get_item),
        21 => const_inner_match_loop::<'a, 21, _, _>(search_hash, base_index, capacity_mask, get_item),
        22 => const_inner_match_loop::<'a, 22, _, _>(search_hash, base_index, capacity_mask, get_item),
        23 => const_inner_match_loop::<'a, 23, _, _>(search_hash, base_index, capacity_mask, get_item),
        24 => const_inner_match_loop::<'a, 24, _, _>(search_hash, base_index, capacity_mask, get_item),
        25 => const_inner_match_loop::<'a, 25, _, _>(search_hash, base_index, capacity_mask, get_item),
        26 => const_inner_match_loop::<'a, 26, _, _>(search_hash, base_index, capacity_mask, get_item),
        27 => const_inner_match_loop::<'a, 27, _, _>(search_hash, base_index, capacity_mask, get_item),
        28 => const_inner_match_loop::<'a, 28, _, _>(search_hash, base_index, capacity_mask, get_item),
        29 => const_inner_match_loop::<'a, 29, _, _>(search_hash, base_index, capacity_mask, get_item),
        30 => const_inner_match_loop::<'a, 30, _, _>(search_hash, base_index, capacity_mask, get_item),
        31 => const_inner_match_loop::<'a, 31, _, _>(search_hash, base_index, capacity_mask, get_item),
        32 => const_inner_match_loop::<'a, 32, _, _>(search_hash, base_index, capacity_mask, get_item),
        33 => const_inner_match_loop::<'a, 33, _, _>(search_hash, base_index, capacity_mask, get_item),
        34 => const_inner_match_loop::<'a, 34, _, _>(search_hash, base_index, capacity_mask, get_item),
        35 => const_inner_match_loop::<'a, 35, _, _>(search_hash, base_index, capacity_mask, get_item),
        36 => const_inner_match_loop::<'a, 36, _, _>(search_hash, base_index, capacity_mask, get_item),
        37 => const_inner_match_loop::<'a, 37, _, _>(search_hash, base_index, capacity_mask, get_item),
        38 => const_inner_match_loop::<'a, 38, _, _>(search_hash, base_index, capacity_mask, get_item),
        39 => const_inner_match_loop::<'a, 39, _, _>(search_hash, base_index, capacity_mask, get_item),
        40 => const_inner_match_loop::<'a, 40, _, _>(search_hash, base_index, capacity_mask, get_item),
        41 => const_inner_match_loop::<'a, 41, _, _>(search_hash, base_index, capacity_mask, get_item),
        42 => const_inner_match_loop::<'a, 42, _, _>(search_hash, base_index, capacity_mask, get_item),
        43 => const_inner_match_loop::<'a, 43, _, _>(search_hash, base_index, capacity_mask, get_item),
        44 => const_inner_match_loop::<'a, 44, _, _>(search_hash, base_index, capacity_mask, get_item),
        45 => const_inner_match_loop::<'a, 45, _, _>(search_hash, base_index, capacity_mask, get_item),
        46 => const_inner_match_loop::<'a, 46, _, _>(search_hash, base_index, capacity_mask, get_item),
        47 => const_inner_match_loop::<'a, 47, _, _>(search_hash, base_index, capacity_mask, get_item),
        48 => const_inner_match_loop::<'a, 48, _, _>(search_hash, base_index, capacity_mask, get_item),
        49 => const_inner_match_loop::<'a, 49, _, _>(search_hash, base_index, capacity_mask, get_item),
        50 => const_inner_match_loop::<'a, 50, _, _>(search_hash, base_index, capacity_mask, get_item),
        51 => const_inner_match_loop::<'a, 51, _, _>(search_hash, base_index, capacity_mask, get_item),
        52 => const_inner_match_loop::<'a, 52, _, _>(search_hash, base_index, capacity_mask, get_item),
        53 => const_inner_match_loop::<'a, 53, _, _>(search_hash, base_index, capacity_mask, get_item),
        54 => const_inner_match_loop::<'a, 54, _, _>(search_hash, base_index, capacity_mask, get_item),
        55 => const_inner_match_loop::<'a, 55, _, _>(search_hash, base_index, capacity_mask, get_item),
        56 => const_inner_match_loop::<'a, 56, _, _>(search_hash, base_index, capacity_mask, get_item),
        57 => const_inner_match_loop::<'a, 57, _, _>(search_hash, base_index, capacity_mask, get_item),
        58 => const_inner_match_loop::<'a, 58, _, _>(search_hash, base_index, capacity_mask, get_item),
        59 => const_inner_match_loop::<'a, 59, _, _>(search_hash, base_index, capacity_mask, get_item),
        60 => const_inner_match_loop::<'a, 60, _, _>(search_hash, base_index, capacity_mask, get_item),
        61 => const_inner_match_loop::<'a, 61, _, _>(search_hash, base_index, capacity_mask, get_item),
        62 => const_inner_match_loop::<'a, 62, _, _>(search_hash, base_index, capacity_mask, get_item),
        63 => const_inner_match_loop::<'a, 63, _, _>(search_hash, base_index, capacity_mask, get_item),
        64 => const_inner_match_loop::<'a, 64, _, _>(search_hash, base_index, capacity_mask, get_item),
        65 => const_inner_match_loop::<'a, 65, _, _>(search_hash, base_index, capacity_mask, get_item),
        66 => const_inner_match_loop::<'a, 66, _, _>(search_hash, base_index, capacity_mask, get_item),
        67 => const_inner_match_loop::<'a, 67, _, _>(search_hash, base_index, capacity_mask, get_item),
        68 => const_inner_match_loop::<'a, 68, _, _>(search_hash, base_index, capacity_mask, get_item),
        69 => const_inner_match_loop::<'a, 69, _, _>(search_hash, base_index, capacity_mask, get_item),
        70 => const_inner_match_loop::<'a, 70, _, _>(search_hash, base_index, capacity_mask, get_item),
        71 => const_inner_match_loop::<'a, 71, _, _>(search_hash, base_index, capacity_mask, get_item),
        72 => const_inner_match_loop::<'a, 72, _, _>(search_hash, base_index, capacity_mask, get_item),
        73 => const_inner_match_loop::<'a, 73, _, _>(search_hash, base_index, capacity_mask, get_item),
        74 => const_inner_match_loop::<'a, 74, _, _>(search_hash, base_index, capacity_mask, get_item),
        75 => const_inner_match_loop::<'a, 75, _, _>(search_hash, base_index, capacity_mask, get_item),
        76 => const_inner_match_loop::<'a, 76, _, _>(search_hash, base_index, capacity_mask, get_item),
        77 => const_inner_match_loop::<'a, 77, _, _>(search_hash, base_index, capacity_mask, get_item),
        78 => const_inner_match_loop::<'a, 78, _, _>(search_hash, base_index, capacity_mask, get_item),
        79 => const_inner_match_loop::<'a, 79, _, _>(search_hash, base_index, capacity_mask, get_item),
        80 => const_inner_match_loop::<'a, 80, _, _>(search_hash, base_index, capacity_mask, get_item),
        81 => const_inner_match_loop::<'a, 81, _, _>(search_hash, base_index, capacity_mask, get_item),
        82 => const_inner_match_loop::<'a, 82, _, _>(search_hash, base_index, capacity_mask, get_item),
        83 => const_inner_match_loop::<'a, 83, _, _>(search_hash, base_index, capacity_mask, get_item),
        84 => const_inner_match_loop::<'a, 84, _, _>(search_hash, base_index, capacity_mask, get_item),
        85 => const_inner_match_loop::<'a, 85, _, _>(search_hash, base_index, capacity_mask, get_item),
        86 => const_inner_match_loop::<'a, 86, _, _>(search_hash, base_index, capacity_mask, get_item),
        87 => const_inner_match_loop::<'a, 87, _, _>(search_hash, base_index, capacity_mask, get_item),
        88 => const_inner_match_loop::<'a, 88, _, _>(search_hash, base_index, capacity_mask, get_item),
        89 => const_inner_match_loop::<'a, 89, _, _>(search_hash, base_index, capacity_mask, get_item),
        90 => const_inner_match_loop::<'a, 90, _, _>(search_hash, base_index, capacity_mask, get_item),
        91 => const_inner_match_loop::<'a, 91, _, _>(search_hash, base_index, capacity_mask, get_item),
        92 => const_inner_match_loop::<'a, 92, _, _>(search_hash, base_index, capacity_mask, get_item),
        93 => const_inner_match_loop::<'a, 93, _, _>(search_hash, base_index, capacity_mask, get_item),
        94 => const_inner_match_loop::<'a, 94, _, _>(search_hash, base_index, capacity_mask, get_item),
        95 => const_inner_match_loop::<'a, 95, _, _>(search_hash, base_index, capacity_mask, get_item),
        96 => const_inner_match_loop::<'a, 96, _, _>(search_hash, base_index, capacity_mask, get_item),
        97 => const_inner_match_loop::<'a, 97, _, _>(search_hash, base_index, capacity_mask, get_item),
        98 => const_inner_match_loop::<'a, 98, _, _>(search_hash, base_index, capacity_mask, get_item),
        99 => const_inner_match_loop::<'a, 99, _, _>(search_hash, base_index, capacity_mask, get_item),
        100 => const_inner_match_loop::<'a, 100, _, _>(search_hash, base_index, capacity_mask, get_item),
        101 => const_inner_match_loop::<'a, 101, _, _>(search_hash, base_index, capacity_mask, get_item),
        102 => const_inner_match_loop::<'a, 102, _, _>(search_hash, base_index, capacity_mask, get_item),
        103 => const_inner_match_loop::<'a, 103, _, _>(search_hash, base_index, capacity_mask, get_item),
        104 => const_inner_match_loop::<'a, 104, _, _>(search_hash, base_index, capacity_mask, get_item),
        105 => const_inner_match_loop::<'a, 105, _, _>(search_hash, base_index, capacity_mask, get_item),
        106 => const_inner_match_loop::<'a, 106, _, _>(search_hash, base_index, capacity_mask, get_item),
        107 => const_inner_match_loop::<'a, 107, _, _>(search_hash, base_index, capacity_mask, get_item),
        108 => const_inner_match_loop::<'a, 108, _, _>(search_hash, base_index, capacity_mask, get_item),
        109 => const_inner_match_loop::<'a, 109, _, _>(search_hash, base_index, capacity_mask, get_item),
        110 => const_inner_match_loop::<'a, 110, _, _>(search_hash, base_index, capacity_mask, get_item),
        111 => const_inner_match_loop::<'a, 111, _, _>(search_hash, base_index, capacity_mask, get_item),
        112 => const_inner_match_loop::<'a, 112, _, _>(search_hash, base_index, capacity_mask, get_item),
        113 => const_inner_match_loop::<'a, 113, _, _>(search_hash, base_index, capacity_mask, get_item),
        114 => const_inner_match_loop::<'a, 114, _, _>(search_hash, base_index, capacity_mask, get_item),
        115 => const_inner_match_loop::<'a, 115, _, _>(search_hash, base_index, capacity_mask, get_item),
        116 => const_inner_match_loop::<'a, 116, _, _>(search_hash, base_index, capacity_mask, get_item),
        117 => const_inner_match_loop::<'a, 117, _, _>(search_hash, base_index, capacity_mask, get_item),
        118 => const_inner_match_loop::<'a, 118, _, _>(search_hash, base_index, capacity_mask, get_item),
        119 => const_inner_match_loop::<'a, 119, _, _>(search_hash, base_index, capacity_mask, get_item),
        120 => const_inner_match_loop::<'a, 120, _, _>(search_hash, base_index, capacity_mask, get_item),
        121 => const_inner_match_loop::<'a, 121, _, _>(search_hash, base_index, capacity_mask, get_item),
        122 => const_inner_match_loop::<'a, 122, _, _>(search_hash, base_index, capacity_mask, get_item),
        123 => const_inner_match_loop::<'a, 123, _, _>(search_hash, base_index, capacity_mask, get_item),
        124 => const_inner_match_loop::<'a, 124, _, _>(search_hash, base_index, capacity_mask, get_item),
        125 => const_inner_match_loop::<'a, 125, _, _>(search_hash, base_index, capacity_mask, get_item),
        126 => const_inner_match_loop::<'a, 126, _, _>(search_hash, base_index, capacity_mask, get_item),
        127 => const_inner_match_loop::<'a, 127, _, _>(search_hash, base_index, capacity_mask, get_item),
        128 => const_inner_match_loop::<'a, 128, _, _>(search_hash, base_index, capacity_mask, get_item),
        129 => const_inner_match_loop::<'a, 129, _, _>(search_hash, base_index, capacity_mask, get_item),
        130 => const_inner_match_loop::<'a, 130, _, _>(search_hash, base_index, capacity_mask, get_item),
        131 => const_inner_match_loop::<'a, 131, _, _>(search_hash, base_index, capacity_mask, get_item),
        132 => const_inner_match_loop::<'a, 132, _, _>(search_hash, base_index, capacity_mask, get_item),
        133 => const_inner_match_loop::<'a, 133, _, _>(search_hash, base_index, capacity_mask, get_item),
        134 => const_inner_match_loop::<'a, 134, _, _>(search_hash, base_index, capacity_mask, get_item),
        135 => const_inner_match_loop::<'a, 135, _, _>(search_hash, base_index, capacity_mask, get_item),
        136 => const_inner_match_loop::<'a, 136, _, _>(search_hash, base_index, capacity_mask, get_item),
        137 => const_inner_match_loop::<'a, 137, _, _>(search_hash, base_index, capacity_mask, get_item),
        138 => const_inner_match_loop::<'a, 138, _, _>(search_hash, base_index, capacity_mask, get_item),
        139 => const_inner_match_loop::<'a, 139, _, _>(search_hash, base_index, capacity_mask, get_item),
        140 => const_inner_match_loop::<'a, 140, _, _>(search_hash, base_index, capacity_mask, get_item),
        141 => const_inner_match_loop::<'a, 141, _, _>(search_hash, base_index, capacity_mask, get_item),
        142 => const_inner_match_loop::<'a, 142, _, _>(search_hash, base_index, capacity_mask, get_item),
        143 => const_inner_match_loop::<'a, 143, _, _>(search_hash, base_index, capacity_mask, get_item),
        144 => const_inner_match_loop::<'a, 144, _, _>(search_hash, base_index, capacity_mask, get_item),
        145 => const_inner_match_loop::<'a, 145, _, _>(search_hash, base_index, capacity_mask, get_item),
        146 => const_inner_match_loop::<'a, 146, _, _>(search_hash, base_index, capacity_mask, get_item),
        147 => const_inner_match_loop::<'a, 147, _, _>(search_hash, base_index, capacity_mask, get_item),
        148 => const_inner_match_loop::<'a, 148, _, _>(search_hash, base_index, capacity_mask, get_item),
        149 => const_inner_match_loop::<'a, 149, _, _>(search_hash, base_index, capacity_mask, get_item),
        150 => const_inner_match_loop::<'a, 150, _, _>(search_hash, base_index, capacity_mask, get_item),
        151 => const_inner_match_loop::<'a, 151, _, _>(search_hash, base_index, capacity_mask, get_item),
        152 => const_inner_match_loop::<'a, 152, _, _>(search_hash, base_index, capacity_mask, get_item),
        153 => const_inner_match_loop::<'a, 153, _, _>(search_hash, base_index, capacity_mask, get_item),
        154 => const_inner_match_loop::<'a, 154, _, _>(search_hash, base_index, capacity_mask, get_item),
        155 => const_inner_match_loop::<'a, 155, _, _>(search_hash, base_index, capacity_mask, get_item),
        156 => const_inner_match_loop::<'a, 156, _, _>(search_hash, base_index, capacity_mask, get_item),
        157 => const_inner_match_loop::<'a, 157, _, _>(search_hash, base_index, capacity_mask, get_item),
        158 => const_inner_match_loop::<'a, 158, _, _>(search_hash, base_index, capacity_mask, get_item),
        159 => const_inner_match_loop::<'a, 159, _, _>(search_hash, base_index, capacity_mask, get_item),
        160 => const_inner_match_loop::<'a, 160, _, _>(search_hash, base_index, capacity_mask, get_item),
        161 => const_inner_match_loop::<'a, 161, _, _>(search_hash, base_index, capacity_mask, get_item),
        162 => const_inner_match_loop::<'a, 162, _, _>(search_hash, base_index, capacity_mask, get_item),
        163 => const_inner_match_loop::<'a, 163, _, _>(search_hash, base_index, capacity_mask, get_item),
        164 => const_inner_match_loop::<'a, 164, _, _>(search_hash, base_index, capacity_mask, get_item),
        165 => const_inner_match_loop::<'a, 165, _, _>(search_hash, base_index, capacity_mask, get_item),
        166 => const_inner_match_loop::<'a, 166, _, _>(search_hash, base_index, capacity_mask, get_item),
        167 => const_inner_match_loop::<'a, 167, _, _>(search_hash, base_index, capacity_mask, get_item),
        168 => const_inner_match_loop::<'a, 168, _, _>(search_hash, base_index, capacity_mask, get_item),
        169 => const_inner_match_loop::<'a, 169, _, _>(search_hash, base_index, capacity_mask, get_item),
        170 => const_inner_match_loop::<'a, 170, _, _>(search_hash, base_index, capacity_mask, get_item),
        171 => const_inner_match_loop::<'a, 171, _, _>(search_hash, base_index, capacity_mask, get_item),
        172 => const_inner_match_loop::<'a, 172, _, _>(search_hash, base_index, capacity_mask, get_item),
        173 => const_inner_match_loop::<'a, 173, _, _>(search_hash, base_index, capacity_mask, get_item),
        174 => const_inner_match_loop::<'a, 174, _, _>(search_hash, base_index, capacity_mask, get_item),
        175 => const_inner_match_loop::<'a, 175, _, _>(search_hash, base_index, capacity_mask, get_item),
        176 => const_inner_match_loop::<'a, 176, _, _>(search_hash, base_index, capacity_mask, get_item),
        177 => const_inner_match_loop::<'a, 177, _, _>(search_hash, base_index, capacity_mask, get_item),
        178 => const_inner_match_loop::<'a, 178, _, _>(search_hash, base_index, capacity_mask, get_item),
        179 => const_inner_match_loop::<'a, 179, _, _>(search_hash, base_index, capacity_mask, get_item),
        180 => const_inner_match_loop::<'a, 180, _, _>(search_hash, base_index, capacity_mask, get_item),
        181 => const_inner_match_loop::<'a, 181, _, _>(search_hash, base_index, capacity_mask, get_item),
        182 => const_inner_match_loop::<'a, 182, _, _>(search_hash, base_index, capacity_mask, get_item),
        183 => const_inner_match_loop::<'a, 183, _, _>(search_hash, base_index, capacity_mask, get_item),
        184 => const_inner_match_loop::<'a, 184, _, _>(search_hash, base_index, capacity_mask, get_item),
        185 => const_inner_match_loop::<'a, 185, _, _>(search_hash, base_index, capacity_mask, get_item),
        186 => const_inner_match_loop::<'a, 186, _, _>(search_hash, base_index, capacity_mask, get_item),
        187 => const_inner_match_loop::<'a, 187, _, _>(search_hash, base_index, capacity_mask, get_item),
        188 => const_inner_match_loop::<'a, 188, _, _>(search_hash, base_index, capacity_mask, get_item),
        189 => const_inner_match_loop::<'a, 189, _, _>(search_hash, base_index, capacity_mask, get_item),
        190 => const_inner_match_loop::<'a, 190, _, _>(search_hash, base_index, capacity_mask, get_item),
        191 => const_inner_match_loop::<'a, 191, _, _>(search_hash, base_index, capacity_mask, get_item),
        192 => const_inner_match_loop::<'a, 192, _, _>(search_hash, base_index, capacity_mask, get_item),
        193 => const_inner_match_loop::<'a, 193, _, _>(search_hash, base_index, capacity_mask, get_item),
        194 => const_inner_match_loop::<'a, 194, _, _>(search_hash, base_index, capacity_mask, get_item),
        195 => const_inner_match_loop::<'a, 195, _, _>(search_hash, base_index, capacity_mask, get_item),
        196 => const_inner_match_loop::<'a, 196, _, _>(search_hash, base_index, capacity_mask, get_item),
        197 => const_inner_match_loop::<'a, 197, _, _>(search_hash, base_index, capacity_mask, get_item),
        198 => const_inner_match_loop::<'a, 198, _, _>(search_hash, base_index, capacity_mask, get_item),
        199 => const_inner_match_loop::<'a, 199, _, _>(search_hash, base_index, capacity_mask, get_item),
        200 => const_inner_match_loop::<'a, 200, _, _>(search_hash, base_index, capacity_mask, get_item),
        201 => const_inner_match_loop::<'a, 201, _, _>(search_hash, base_index, capacity_mask, get_item),
        202 => const_inner_match_loop::<'a, 202, _, _>(search_hash, base_index, capacity_mask, get_item),
        203 => const_inner_match_loop::<'a, 203, _, _>(search_hash, base_index, capacity_mask, get_item),
        204 => const_inner_match_loop::<'a, 204, _, _>(search_hash, base_index, capacity_mask, get_item),
        205 => const_inner_match_loop::<'a, 205, _, _>(search_hash, base_index, capacity_mask, get_item),
        206 => const_inner_match_loop::<'a, 206, _, _>(search_hash, base_index, capacity_mask, get_item),
        207 => const_inner_match_loop::<'a, 207, _, _>(search_hash, base_index, capacity_mask, get_item),
        208 => const_inner_match_loop::<'a, 208, _, _>(search_hash, base_index, capacity_mask, get_item),
        209 => const_inner_match_loop::<'a, 209, _, _>(search_hash, base_index, capacity_mask, get_item),
        210 => const_inner_match_loop::<'a, 210, _, _>(search_hash, base_index, capacity_mask, get_item),
        211 => const_inner_match_loop::<'a, 211, _, _>(search_hash, base_index, capacity_mask, get_item),
        212 => const_inner_match_loop::<'a, 212, _, _>(search_hash, base_index, capacity_mask, get_item),
        213 => const_inner_match_loop::<'a, 213, _, _>(search_hash, base_index, capacity_mask, get_item),
        214 => const_inner_match_loop::<'a, 214, _, _>(search_hash, base_index, capacity_mask, get_item),
        215 => const_inner_match_loop::<'a, 215, _, _>(search_hash, base_index, capacity_mask, get_item),
        216 => const_inner_match_loop::<'a, 216, _, _>(search_hash, base_index, capacity_mask, get_item),
        217 => const_inner_match_loop::<'a, 217, _, _>(search_hash, base_index, capacity_mask, get_item),
        218 => const_inner_match_loop::<'a, 218, _, _>(search_hash, base_index, capacity_mask, get_item),
        219 => const_inner_match_loop::<'a, 219, _, _>(search_hash, base_index, capacity_mask, get_item),
        220 => const_inner_match_loop::<'a, 220, _, _>(search_hash, base_index, capacity_mask, get_item),
        221 => const_inner_match_loop::<'a, 221, _, _>(search_hash, base_index, capacity_mask, get_item),
        222 => const_inner_match_loop::<'a, 222, _, _>(search_hash, base_index, capacity_mask, get_item),
        223 => const_inner_match_loop::<'a, 223, _, _>(search_hash, base_index, capacity_mask, get_item),
        224 => const_inner_match_loop::<'a, 224, _, _>(search_hash, base_index, capacity_mask, get_item),
        225 => const_inner_match_loop::<'a, 225, _, _>(search_hash, base_index, capacity_mask, get_item),
        226 => const_inner_match_loop::<'a, 226, _, _>(search_hash, base_index, capacity_mask, get_item),
        227 => const_inner_match_loop::<'a, 227, _, _>(search_hash, base_index, capacity_mask, get_item),
        228 => const_inner_match_loop::<'a, 228, _, _>(search_hash, base_index, capacity_mask, get_item),
        229 => const_inner_match_loop::<'a, 229, _, _>(search_hash, base_index, capacity_mask, get_item),
        230 => const_inner_match_loop::<'a, 230, _, _>(search_hash, base_index, capacity_mask, get_item),
        231 => const_inner_match_loop::<'a, 231, _, _>(search_hash, base_index, capacity_mask, get_item),
        232 => const_inner_match_loop::<'a, 232, _, _>(search_hash, base_index, capacity_mask, get_item),
        233 => const_inner_match_loop::<'a, 233, _, _>(search_hash, base_index, capacity_mask, get_item),
        234 => const_inner_match_loop::<'a, 234, _, _>(search_hash, base_index, capacity_mask, get_item),
        235 => const_inner_match_loop::<'a, 235, _, _>(search_hash, base_index, capacity_mask, get_item),
        236 => const_inner_match_loop::<'a, 236, _, _>(search_hash, base_index, capacity_mask, get_item),
        237 => const_inner_match_loop::<'a, 237, _, _>(search_hash, base_index, capacity_mask, get_item),
        238 => const_inner_match_loop::<'a, 238, _, _>(search_hash, base_index, capacity_mask, get_item),
        239 => const_inner_match_loop::<'a, 239, _, _>(search_hash, base_index, capacity_mask, get_item),
        240 => const_inner_match_loop::<'a, 240, _, _>(search_hash, base_index, capacity_mask, get_item),
        241 => const_inner_match_loop::<'a, 241, _, _>(search_hash, base_index, capacity_mask, get_item),
        242 => const_inner_match_loop::<'a, 242, _, _>(search_hash, base_index, capacity_mask, get_item),
        243 => const_inner_match_loop::<'a, 243, _, _>(search_hash, base_index, capacity_mask, get_item),
        244 => const_inner_match_loop::<'a, 244, _, _>(search_hash, base_index, capacity_mask, get_item),
        245 => const_inner_match_loop::<'a, 245, _, _>(search_hash, base_index, capacity_mask, get_item),
        246 => const_inner_match_loop::<'a, 246, _, _>(search_hash, base_index, capacity_mask, get_item),
        247 => const_inner_match_loop::<'a, 247, _, _>(search_hash, base_index, capacity_mask, get_item),
        248 => const_inner_match_loop::<'a, 248, _, _>(search_hash, base_index, capacity_mask, get_item),
        249 => const_inner_match_loop::<'a, 249, _, _>(search_hash, base_index, capacity_mask, get_item),
        250 => const_inner_match_loop::<'a, 250, _, _>(search_hash, base_index, capacity_mask, get_item),
        251 => const_inner_match_loop::<'a, 251, _, _>(search_hash, base_index, capacity_mask, get_item),
        252 => const_inner_match_loop::<'a, 252, _, _>(search_hash, base_index, capacity_mask, get_item),
        253 => const_inner_match_loop::<'a, 253, _, _>(search_hash, base_index, capacity_mask, get_item),
        254 => const_inner_match_loop::<'a, 254, _, _>(search_hash, base_index, capacity_mask, get_item),
        255 => const_inner_match_loop::<'a, 255, _, _>(search_hash, base_index, capacity_mask, get_item),
        _ => unreachable!(),
    }
}

// #[no_mangle]
// pub fn use_const_inner_match_loop<'a>(result: u8, search_hash: u64, base_index: usize, capacity_mask: usize, data: &'a [(u64, usize)]) -> Option<&'a usize> {
//     assert!(result < 4);
//
//     match result {
//         0 => const_inner_match_loop::<'a, 0, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         1 => const_inner_match_loop::<'a, 1, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         2 => const_inner_match_loop::<'a, 2, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         3 => const_inner_match_loop::<'a, 3, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         4 => const_inner_match_loop::<'a, 4, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         5 => const_inner_match_loop::<'a, 5, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         6 => const_inner_match_loop::<'a, 6, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         7 => const_inner_match_loop::<'a, 7, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         8 => const_inner_match_loop::<'a, 8, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         9 => const_inner_match_loop::<'a, 9, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         10 => const_inner_match_loop::<'a, 10, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         11 => const_inner_match_loop::<'a, 11, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         12 => const_inner_match_loop::<'a, 12, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         13 => const_inner_match_loop::<'a, 13, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         14 => const_inner_match_loop::<'a, 14, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         15 => const_inner_match_loop::<'a, 15, _, usize>(search_hash, base_index, capacity_mask, |i| &data[i]),
//         _ => unreachable!(),
//     }
// }

// pub fn extract(value: u64) -> u8 {
//     let mut result = 0u8;
//     for i in 0..8 {
//         result |= (value >> (i * 8)) as u8 & 1;
//     }
//     result
// }
//
// pub fn compress<const M: u32>(mut x: u32) -> u32 {
//     let mut m = M;
//     let mut mk: u32 = 0;
//     let mut mp: u32 = 0;
//     let mut mv: u32 = 0;
//     let mut t: u32 = 0;
//     x = x & m; // Clear irrelevant bits.
//     mk = !m << 1; // We will count 0's to right.
//     for i in 0..5 {
//         mp = mk ^ (mk << 1); // Parallel suffix.
//         mp = mp ^ (mp << 2);
//         mp = mp ^ (mp << 4);
//         mp = mp ^ (mp << 8);
//         mp = mp ^ (mp << 16);
//         mv = mp & m; // Bits to move.
//         m = m ^ mv | (mv >> (1 << i)); // Compress m.
//         t = x & mv;
//         x = x ^ t | (t >> (1 << i)); // Compress x.
//         mk = mk & !mp;
//     }
//     x
// }
//
// #[cfg(test)]
// mod tests {
//     #[test]
//     pub fn test_compress() {
//         const MASK: u32 = 0b0001_0001_0001_0001_0001_0001_0001_0001;
//         let value = 0b0001_0001_0101_1111_1100_0000_1111_0110u32;
//
//         assert_eq!(super::compress::<MASK>(value), 0b1111_0010);
//     }
// }
