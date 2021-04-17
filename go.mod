module github.com/nicolagi/muscle

go 1.16

require (
	github.com/fortytw2/leaktest v1.3.0
	github.com/google/go-cmp v0.5.5
	github.com/google/gops v0.3.17
	github.com/lionkov/go9p v0.0.0-20190125202718-b4200817c487
	github.com/nicolagi/signit v0.0.0-20210417064458-ac85470c0fc0
	github.com/stretchr/testify v1.7.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

replace github.com/lionkov/go9p v0.0.0-20190125202718-b4200817c487 => github.com/nicolagi/go9p v0.0.0-20190223213930-d791c5b05663
