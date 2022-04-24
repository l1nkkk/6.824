package logger

import (
	"fmt"
	"log"
	"os"
)

var (
	Info *log.Logger
	Warning *log.Logger
	Error * log.Logger
	Panic * log.Logger
)

func init(){
	//errFile,err:=os.OpenFile("errors.logger",os.O_CREATE|os.O_WRONLY|os.O_APPEND,0666)
	//if err!=nil{
	//	logger.Fatalln("打开日志文件失败：",err)
	//}

	infoPre := fmt.Sprintf("%-8s%-6d", "Info:", os.Getppid())
	Info = log.New(os.Stdout, infoPre,log.Ldate | log.Lmicroseconds | log.Lshortfile)

	warnPre := fmt.Sprintf("%-8s%-6d", "Warning:", os.Getppid())
	Warning = log.New(os.Stdout, warnPre, log.Ldate | log.Lmicroseconds | log.Lshortfile)

	errorPre := fmt.Sprintf("%-8s%-6d", "Error:", os.Getppid())
	Error = log.New(os.Stderr, errorPre, log.Ldate | log.Lmicroseconds | log.Lshortfile)

	panicPre := fmt.Sprintf("%-8s%-6d", "Panic:", os.Getppid())
	Panic = log.New(os.Stderr, panicPre, log.Ldate | log.Lmicroseconds | log.Lshortfile)
	//Error = logger.New(io.MultiWriter(os.Stderr,errFile),"Error:",logger.Ldate | logger.Ltime | logger.Lshortfile)

}