Here we will package all the resource file into a go file.

So the project's final result is a single execution file.

When debug:
go-bindata -pkg res -debug -o res.go ./...

Relaease:
go-bindata -pkg res -o res.go ./...