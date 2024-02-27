git clone https://github.com/moogacs/nilaway-plugin.git
cd nilaway-plugin
go mod init github.com/nilaway-plugin
go mod tidy
go version -m $(which golangci-lint)
go build -o '${{ github.workspace }}/.plugins/nilaway.so'  -buildmode=plugin plugin/nilaway.go          
echo ${{ github.workspace }}