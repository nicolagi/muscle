module github.com/nicolagi/muscle

go 1.16

require (
	github.com/fortytw2/leaktest v1.3.0
	github.com/google/go-cmp v0.3.1
	github.com/google/gops v0.3.12
	github.com/kr/pretty v0.1.0 // indirect
	github.com/lionkov/go9p v0.0.0-20190125202718-b4200817c487
	github.com/nicolagi/signit v0.0.0-20210417064458-ac85470c0fc0
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.6.1
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20200909081042-eff7692f9009 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776 // indirect
)

replace github.com/lionkov/go9p v0.0.0-20190125202718-b4200817c487 => github.com/nicolagi/go9p v0.0.0-20190223213930-d791c5b05663
