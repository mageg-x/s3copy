package utils

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"

	"github.com/mattn/go-isatty"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var mu sync.Mutex
var loggers = make(map[string]*logHandle)

type logHandle struct {
	logrus.Logger

	name     string
	logid    string
	lvl      *logrus.Level
	colorful bool
}

func (l *logHandle) Format(e *logrus.Entry) ([]byte, error) {
	lvl := e.Level
	if l.lvl != nil {
		lvl = *l.lvl
	}
	lvlStr := strings.ToUpper(lvl.String())
	if l.colorful {
		var color int
		switch lvl {
		case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
			color = 31 // RED
		case logrus.WarnLevel:
			color = 33 // YELLOW
		case logrus.InfoLevel:
			color = 34 // BLUE
		default: // logrus.TraceLevel, logrus.DebugLevel
			color = 35 // MAGENTA
		}
		lvlStr = fmt.Sprintf("\033[1;%dm%s\033[0m", color, lvlStr)
	}
	const timeFormat = "2006/01/02 15:04:05.000000"
	timestamp := e.Time.Format(timeFormat)
	str := fmt.Sprintf("%s%v %s[%d] <%v>: %v [%s:%d]",
		l.logid,
		timestamp,
		l.name,
		os.Getpid(),
		lvlStr,
		strings.TrimRight(e.Message, "\n"),
		path.Base(e.Caller.File),
		e.Caller.Line)

	if len(e.Data) != 0 {
		str += " " + fmt.Sprint(e.Data)
	}
	if !strings.HasSuffix(str, "\n") {
		str += "\n"
	}
	return []byte(str), nil
}

// for aws.Logger
func (l *logHandle) Log(args ...interface{}) {
	l.Debugln(args...)
}

// 支持 ANSI 颜色检测
// 支持 ANSI 颜色检测
func supportANSIColor(fd uintptr) bool {
	return isatty.IsTerminal(fd) && runtime.GOOS != "windows"
}

func newLogger(name string) *logHandle {
	l := &logHandle{Logger: *logrus.New(), name: name, colorful: supportANSIColor(os.Stderr.Fd())}
	l.Formatter = l
	l.SetReportCaller(true)
	return l
}

// GetLogger returns a logger mapped to `name`
func GetLogger(name string) *logHandle {
	mu.Lock()
	defer mu.Unlock()

	if logger, ok := loggers[name]; ok {
		return logger
	}
	logger := newLogger(name)
	loggers[name] = logger

	return logger
}

// SetLogLevel sets Level to all the loggers in the map
func SetLogLevel(lvl logrus.Level) {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.Level = lvl
	}
}

func DisableLogColor() {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.colorful = false
	}
}

// 修改后的 SetOutFile 函数，使用 lumberjack 实现日志轮转
func SetOutFile(name string) {
	// 创建 lumberjack logger，用于日志轮转
	logWriter := &lumberjack.Logger{
		Filename:   name, // 日志文件路径
		MaxSize:    500,  // megabytes 每个日志文件的最大大小
		MaxBackups: 3,    // 保留旧文件的数量
		MaxAge:     28,   // days 保留旧文件的天数
		Compress:   true, // 是否压缩/归档旧文件
	}

	// 设置日志输出为多写入器：文件 + 控制台
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		// logger.SetOutput(io.MultiWriter(logWriter, os.Stdout)) // 同时输出到文件和控制台
		logger.SetOutput(logWriter)
		logger.colorful = false
	}
}

func SetOutput(w io.Writer) {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.SetOutput(w)
	}
}

func SetLogID(id string) {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.logid = id
	}
}
