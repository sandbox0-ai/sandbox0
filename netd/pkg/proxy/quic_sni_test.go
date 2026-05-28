package proxy

import (
	"encoding/hex"
	"strings"
	"testing"
	"unicode"
)

// From https://pkg.go.dev/github.com/cuonglm/quicsni#example-ReadClientHello
var clientInitial = mustHexDecodeString(`
cdff000020088394c8f03e5157080000 449e9cdb990bfb66bc6a93032b50dd89
73972d149421874d3849e3708d71354e a33bcdc356f3ea6e2a1a1bd7c3d14003
8d3e784d04c30a2cdb40c32523aba2da fe1c1bf3d27a6be38fe38ae033fbb071
3c1c73661bb6639795b42b97f77068ea d51f11fbf9489af2501d09481e6c64d4
b8551cd3cea70d830ce2aeeec789ef55 1a7fbe36b3f7e1549a9f8d8e153b3fac
3fb7b7812c9ed7c20b4be190ebd89956 26e7f0fc887925ec6f0606c5d36aa81b
ebb7aacdc4a31bb5f23d55faef5c5190 5783384f375a43235b5c742c78ab1bae
0a188b75efbde6b3774ed61282f9670a 9dea19e1566103ce675ab4e21081fb58
60340a1e88e4f10e39eae25cd685b109 29636d4f02e7fad2a5a458249f5c0298
a6d53acbe41a7fc83fa7cc01973f7a74 d1237a51974e097636b6203997f921d0
7bc1940a6f2d0de9f5a11432946159ed 6cc21df65c4ddd1115f86427259a196c
7148b25b6478b0dc7766e1c4d1b1f515 9f90eabc61636226244642ee148b464c
9e619ee50a5e3ddc836227cad938987c 4ea3c1fa7c75bbf88d89e9ada642b2b8
8fe8107b7ea375b1b64889a4e9e5c38a 1c896ce275a5658d250e2d76e1ed3a34
ce7e3a3f383d0c996d0bed106c2899ca 6fc263ef0455e74bb6ac1640ea7bfedc
59f03fee0e1725ea150ff4d69a7660c5 542119c71de270ae7c3ecfd1af2c4ce5
51986949cc34a66b3e216bfe18b347e6 c05fd050f85912db303a8f054ec23e38
f44d1c725ab641ae929fecc8e3cefa56 19df4231f5b4c009fa0c0bbc60bc75f7
6d06ef154fc8577077d9d6a1d2bd9bf0 81dc783ece60111bea7da9e5a9748069
d078b2bef48de04cabe3755b197d52b3 2046949ecaa310274b4aac0d008b1948
c1082cdfe2083e386d4fd84c0ed0666d 3ee26c4515c4fee73433ac703b690a9f
7bf278a77486ace44c489a0c7ac8dfe4 d1a58fb3a730b993ff0f0d61b4d89557
831eb4c752ffd39c10f6b9f46d8db278 da624fd800e4af85548a294c1518893a
8778c4f6d6d73c93df200960104e062b 388ea97dcf4016bced7f62b4f062cb6c
04c20693d9a0e3b74ba8fe74cc012378 84f40d765ae56a51688d985cf0ceaef4
3045ed8c3f0c33bced08537f6882613a cd3b08d665fce9dd8aa73171e2d3771a
61dba2790e491d413d93d987e2745af2 9418e428be34941485c93447520ffe23
1da2304d6a0fd5d07d08372202369661 59bef3cf904d722324dd852513df39ae
030d8173908da6364786d3c1bfcb19ea 77a63b25f1e7fc661def480c5d00d444
56269ebd84efd8e3a8b2c257eec76060 682848cbf5194bc99e49ee75e4d0d254
bad4bfd74970c30e44b65511d4ad0e6e c7398e08e01307eeeea14e46ccd87cf3
6b285221254d8fc6a6765c524ded0085 dca5bd688ddf722e2c0faf9d0fb2ce7a
0c3f2cee19ca0ffba461ca8dc5d2c817 8b0762cf67135558494d2a96f1a139f0
edb42d2af89a9c9122b07acbc29e5e72 2df8615c343702491098478a389c9872
a10b0c9875125e257c7bfdf27eef4060 bd3d00f4c14fd3e3496c38d3c5d1a566
8c39350effbc2d16ca17be4ce29f02ed 969504dda2a8c6b9ff919e693ee79e09
089316e7d1d89ec099db3b2b268725d8 88536a4b8bf9aee8fb43e82a4d919d48
b5a464ca5b62df3be35ee0d0a2ec68f3
`)

func TestParseUDPSNIFromQUIC(t *testing.T) {
	reassembler := newQuicReassembler()
	sni := reassembler.ParseSNI(clientInitial, "1.1.1.1", "2.2.2.2")
	if sni != "example.com" {
		t.Fatalf("expected example.com, got %q", sni)
	}
}

func TestParseUDPSNIIgnoresMalformedInitial(t *testing.T) {
	reassembler := newQuicReassembler()
	malformed := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	if sni := reassembler.ParseSNI(malformed, "10.0.0.2", "10.0.0.53"); sni != "" {
		t.Fatalf("expected malformed packet to have no SNI, got %q", sni)
	}
}

func mustHexDecodeString(s string) []byte {
	normalized := strings.Map(func(c rune) rune {
		if unicode.IsSpace(c) {
			return -1
		}
		return c
	}, s)
	out, err := hex.DecodeString(normalized)
	if err != nil {
		panic(err)
	}
	return out
}
