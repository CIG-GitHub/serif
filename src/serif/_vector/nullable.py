# Claude - i want your help building this. Two cases. Memory efficient and speed efficient.
# Not even sure if bytes is the correct way to do this, but it is a start. We can always change the underlying implementation later.
# Text autcomplete has gotten insane...
class ByteMask:
    def is_null(self, idx): return bool(self._mask[idx])

class BitMask:
    """ pack bits into bytes, so 8 values per byte """

    def is_null(self, idx):
        byte_idx = idx // 8
        bit_idx = idx % 8
        return bool((self._mask[byte_idx] >> bit_idx) & 1)