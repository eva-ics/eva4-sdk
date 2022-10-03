pub trait BitMan {
    fn get_bit(self, bit: u32) -> bool;
    fn with_bit(self, bit: u32, value: bool) -> Self;
}
macro_rules! impl_bitman {
    (for $($tp:ident),+) => {
        $(impl BitMan for $tp {
            fn get_bit(self, bit: u32) -> bool {
                if bit >= $tp::BITS { false } else { self >> bit & 1 != 0 }
            }
            fn with_bit(self, bit: u32, value: bool) -> Self {
                if bit < $tp::BITS {
                    if value { self | 1 << bit } else { self & !(1 << bit) }
                } else { self }
            }
        })+
    };
}
impl_bitman!(for u8, u16, u32, u64, u128);
