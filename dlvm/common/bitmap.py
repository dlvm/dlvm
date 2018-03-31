from typing import List, Any
import array


class BitMap():
    """
    BitMap class
    """

    BITMASK = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80]
    BIT_CNT = [bin(i).count("1") for i in range(256)]

    def __init__(self, maxnum: int=0)-> None:
        """
        Create a BitMap
        """
        nbytes = (maxnum + 7) // 8
        with open('/tmp/t1', 'w') as f:
            f.write(str(maxnum))
            f.write('\n')
            f.write(str(nbytes))
            f.write('\n')
        self.bitmap = array.array('B', [0 for i in range(nbytes)])

    def __del__(self)-> None:
        """
        Destroy the BitMap
        """
        pass

    def set(self, pos: int)-> None:
        """
        Set the value of bit@pos to 1
        """
        self.bitmap[pos // 8] |= self.BITMASK[pos % 8]

    def reset(self, pos: int)-> None:
        """
        Reset the value of bit@pos to 0
        """
        self.bitmap[pos // 8] &= ~self.BITMASK[pos % 8]

    def flip(self, pos: int)-> None:
        """
        Flip the value of bit@pos
        """
        self.bitmap[pos // 8] ^= self.BITMASK[pos % 8]

    def count(self)-> int:
        """
        Count bits set
        """
        return sum([self.BIT_CNT[x] for x in self.bitmap])

    def size(self)-> int:
        """
        Return size
        """
        return len(self.bitmap) * 8

    def test(self, pos: int)-> bool:
        """
        Return bit value
        """
        return (self.bitmap[pos // 8] & self.BITMASK[pos % 8]) != 0

    def any(self)-> bool:
        """
        Test if any bit is set
        """
        return self.count() > 0

    def none(self)-> bool:
        """
        Test if no bit is set
        """
        return self.count() == 0

    def all(self)-> bool:
        """
        Test if all bits are set
        """
        return (self.count() + 7) // 8 * 8 == self.size()

    def nonzero(self)-> List[int]:
        """
        Get all non-zero bits
        """
        return [i for i in range(self.size()) if self.test(i)]

    def tostring(self)-> str:
        """
        Convert BitMap to string
        """
        return "".join([("%s" % bin(x)[2:]).zfill(8)
                        for x in self.bitmap[::-1]])

    def __str__(self)-> str:
        """
        Overloads string operator
        """
        return self.tostring()

    def __getitem__(self, item: int)-> bool:
        """
        Return a bit when indexing like a array
        """
        return self.test(item)

    def __setitem__(self, key: int, value: bool)-> None:
        """
        Sets a bit when indexing like a array
        """
        if value is True:
            self.set(key)
        elif value is False:
            self.reset(key)
        else:
            raise Exception("Use a boolean value to assign to a bitfield")

    def tohexstring(self)-> str:
        """
        Returns a hexadecimal string
        """
        val = self.tostring()
        st = "{0:0x}".format(int(val, 2))
        return st.zfill(len(self.bitmap)*2)

    @classmethod
    def fromhexstring(cls: Any, hexstring: str)-> Any:
        """
        Construct BitMap from hex string
        """
        bitstring = format(
            int(hexstring, 16), "0" + str(len(hexstring)//4) + "b")
        return cls.fromstring(bitstring)

    @classmethod
    def fromstring(cls: type, bitstring: str)-> Any:
        """
        Construct BitMap from string
        """
        nbits = len(bitstring)
        bm = cls(nbits)
        for i in range(nbits):
            if bitstring[-i-1] == '1':
                bm.set(i)
            elif bitstring[-i-1] != '0':
                raise Exception("Invalid bit string!")
        return bm
