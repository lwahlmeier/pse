package lcwlog

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lwahlmeier/pyfmt"
)

type Logger interface {
	Debug(...interface{})
	Warn(...interface{})
	Fatal(...interface{})
}

// LCWLogger this struct is generic generic logger used.
type LCWLogger interface {
	Trace(...interface{})
	Debug(...interface{})
	Verbose(...interface{})
	Info(...interface{})
	Warn(...interface{})
	Fatal(...interface{})
	GetLogLevel() Level
}
type LCWLoggerConfig interface {
	SetLogger(Logger)
	SetLevel(Level)
	SetDateFormat(string)
	AddLogFile(string, Level) error
	RemoveLogFile(string)
	ForceFlush(bool)
	Flush()
	EnableLevelLogging(bool)
	EnableTimeLogging(bool)
}

// Level is the Level of logging set
type Level int32

const (
	defaultLevel Level = -1
	//FatalLevel this is used to log an error that will cause fatal problems in the program
	FatalLevel Level = 0
	//WarnLevel is logging for interesting events that need to be known about but are not crazy
	WarnLevel    Level = 20
	InfoLevel    Level = 30
	VerboseLevel Level = 40
	DebugLevel   Level = 50
	TraceLevel   Level = 60
)

type logFile struct {
	path     string
	logLevel Level
	fp       *os.File
}

type logMessage struct {
	logLevel Level
	ltime    time.Time
	msg      string
	args     []interface{}
	wg       *sync.WaitGroup
}

type fullLCWLogger struct {
	setLogger    Logger
	currentLevel Level
	highestLevel Level
	dateFMT      string
	logfiles     sync.Map
	logQueue     chan *logMessage
	forceFlush   *atomic.Bool
	logLevel     bool
	logTime      bool
}

var logger atomic.Pointer[fullLCWLogger]

var prefixLogger = sync.Map{}

const traceMsg = "[ TRACE ]"
const debugMsg = "[ DEBUG ]"
const warnMsg = "[ WARN  ]"
const fatalMsg = "[ FATAL ]"
const infoMsg = "[ INFO  ]"
const verboseMsg = "[VERBOSE]"
const dateFMT = "2006-01-02 15:04:05.999"

func resetLogger() {
	logger.Store(nil)
	prefixLogger.Range(func(k, v any) bool {
		prefixLogger.Delete(k)
		return true
	})
}

func GetLoggerConfig() LCWLoggerConfig {
	GetLogger()
	return logger.Load()
}

//GetLogger gets a logger for logging.
func GetLogger() *fullLCWLogger {
	ll := logger.Load()
	if ll != nil {
		return ll
	}

	ab := &atomic.Bool{}
	ab.Store(true)
	nll := &fullLCWLogger{
		currentLevel: InfoLevel,
		highestLevel: InfoLevel,
		dateFMT:      dateFMT,
		logQueue:     make(chan *logMessage, 1000),
		logfiles:     sync.Map{},
		forceFlush:   ab,
		logLevel:     true,
		logTime:      true,
	}
	nll.AddLogFile("STDOUT", defaultLevel)
	if logger.CompareAndSwap(ll, nll) {
		go nll.writeLogQueue()
		return nll
	}
	return logger.Load()
}

//GetLoggerWithPrefix gets a logger for logging with a static prefix.
func GetLoggerWithPrefix(prefix string) LCWLogger {
	baseLogger := GetLogger()
	if prefix == "" {
		return baseLogger
	}
	cpl, exists := prefixLogger.Load(prefix)
	if exists {
		return cpl.(LCWLogger)
	}
	npl := &lcwPrefixLogger{LCWLogger: baseLogger, prefix: prefix}

	cpl, _ = prefixLogger.LoadOrStore(prefix, npl)
	return cpl.(LCWLogger)
}

//GetLogLevel gets the current highest set log level for lcwlog
func (LCWLogger *fullLCWLogger) GetLogLevel() Level {
	return LCWLogger.highestLevel
}

//EnableLevelLogging enables/disables logging of the level (WARN/DEBUG, etc)
func (LCWLogger *fullLCWLogger) EnableLevelLogging(b bool) {
	LCWLogger.logLevel = b
}

//EnableTimeLogging enables/disables logging of the timestamp
func (LCWLogger *fullLCWLogger) EnableTimeLogging(b bool) {
	LCWLogger.logTime = b
}

//RemoveLogFile removes logging of a file (can be STDOUT/STDERR too)
func (LCWLogger *fullLCWLogger) RemoveLogFile(file string) {
	_, ok := LCWLogger.logfiles.Load(file)
	if ok {
		highestLL := defaultLevel
		LCWLogger.logfiles.Delete(file)
		LCWLogger.logfiles.Range(func(key, v any) bool {
			lf := v.(*logFile)
			if lf.logLevel > highestLL {
				highestLL = lf.logLevel
			}
			return true
		})
		if highestLL > defaultLevel {
			LCWLogger.highestLevel = highestLL
		}
	}
}

//AddLogFile adds logging of a file (can be STDOUT/STDERR too)
func (LCWLogger *fullLCWLogger) AddLogFile(file string, logLevel Level) error {
	var fp *os.File
	var err error
	if file == "STDOUT" {
		fp = os.Stdout
	} else if file == "STDERR" {
		fp = os.Stderr
	} else {
		fp, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0750)
		if err != nil {
			return err
		}
		fs, err := fp.Stat()
		if err != nil {
			return err
		}
		fp.Seek(fs.Size(), 0)
	}
	if logLevel > LCWLogger.highestLevel {
		LCWLogger.highestLevel = logLevel
	}
	LCWLogger.logfiles.Store(file, &logFile{path: file, logLevel: logLevel, fp: fp})
	return nil
}

func (LCWLogger *fullLCWLogger) writeLogQueue() {
	syncSoon := time.Duration(100 * time.Millisecond)
	syncLate := time.Duration(10000 * time.Millisecond)
	delay := time.NewTimer(syncSoon)
	if LCWLogger.forceFlush.Load() {
		delay.Reset(syncLate)
	}
	defer delay.Stop()
	for {
		if LCWLogger.forceFlush.Load() {
			delay.Reset(syncLate)
		} else {
			delay.Reset(syncSoon)
		}
		select {
		case lm := <-LCWLogger.logQueue:
			LCWLogger.writeLogs(lm.logLevel, lm.wg != nil, LCWLogger.formatLogMessage(lm))
			if lm.wg != nil {
				lm.wg.Done()
			}
		case <-delay.C:
			LCWLogger.logfiles.Range(func(key, v any) bool {
				lf := v.(*logFile)
				lf.fp.Sync()
				return true
			})
		}
	}
}

func (LCWLogger *fullLCWLogger) formatLogMessage(lm *logMessage) string {
	var sb strings.Builder
	if LCWLogger.logTime {
		sb.WriteString(lm.ltime.Format(dateFMT))
		sb.WriteString("\t")
	}
	if LCWLogger.logLevel {
		if lm.logLevel == FatalLevel {
			sb.WriteString(fatalMsg)
		} else if lm.logLevel == WarnLevel {
			sb.WriteString(warnMsg)
		} else if lm.logLevel == InfoLevel {
			sb.WriteString(infoMsg)
		} else if lm.logLevel == VerboseLevel {
			sb.WriteString(verboseMsg)
		} else if lm.logLevel == DebugLevel {
			sb.WriteString(debugMsg)
		} else if lm.logLevel == TraceLevel {
			sb.WriteString(traceMsg)
		} else {
			sb.WriteString(strconv.FormatInt(int64(lm.logLevel), 10))
		}
		sb.WriteString("\t")
	}
	sb.WriteString(pyfmt.Must(lm.msg, lm.args...))
	// sb.WriteString(argFormatter1(lm.msg, lm.args...))
	sb.WriteString("\n")
	return sb.String()
}

func (LCWLogger *fullLCWLogger) writeLogs(logLevel Level, sync bool, msg string) {
	LCWLogger.logfiles.Range(func(k, v any) bool {
		lgr := v.(*logFile)
		if lgr.logLevel >= logLevel || (lgr.logLevel == defaultLevel && LCWLogger.currentLevel >= logLevel) {
			lgr.fp.WriteString(msg)
			if LCWLogger.forceFlush.Load() || sync {
				lgr.fp.Sync()
			}
		}
		return true
	})
}

func (LCWLogger *fullLCWLogger) wrapMessage(ll Level, wg *sync.WaitGroup, args ...interface{}) *logMessage {
	st := time.Now()
	var msg string
	switch args[0].(type) {
	case string:
		msg = args[0].(string)
	default:
		msg = fmt.Sprintf("%v", args[0])
	}
	return &logMessage{
		ltime:    st,
		msg:      msg,
		args:     args[1:],
		logLevel: ll,
		wg:       wg,
	}
}

//Forces logger to write all current logs, will block till done
func (LCWLogger *fullLCWLogger) Flush() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	LCWLogger.logQueue <- LCWLogger.wrapMessage(TraceLevel, wg, "Flush")
	wg.Wait()
}

//ForceFlush enables/disables forcing sync on logfiles after every write
func (LCWLogger *fullLCWLogger) ForceFlush(ff bool) {
	LCWLogger.forceFlush.Store(ff)
	if ff {
		LCWLogger.Flush()
	}
}

//SetDateFormat allows you to set how the date/time is formated
func (LCWLogger *fullLCWLogger) SetDateFormat(nf string) {
	LCWLogger.dateFMT = nf
}

// SetLogger takes a structured logger to interface with.
// After the logger is setup it will be available across your packages
// If SetLogger is not used Debug will not create output
func (LCWLogger *fullLCWLogger) SetLogger(givenLogger Logger) {
	LCWLogger.setLogger = givenLogger
}

// SetLevel sets the LCWLogger log level.
func (LCWLogger *fullLCWLogger) SetLevel(level Level) {
	LCWLogger.currentLevel = level
	hl := level
	LCWLogger.logfiles.Range(func(k, v any) bool {
		lgr := v.(*logFile)
		if lgr.logLevel > hl {
			hl = lgr.logLevel
		}
		return true
	})
	LCWLogger.highestLevel = hl
}

// Debug logs a message at level Debug on the standard logger.
func (LCWLogger *fullLCWLogger) Debug(message ...interface{}) {
	if LCWLogger.highestLevel >= DebugLevel {
		if LCWLogger.setLogger == nil {
			if LCWLogger.forceFlush.Load() {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				LCWLogger.logQueue <- LCWLogger.wrapMessage(DebugLevel, wg, message...)
				wg.Wait()
			} else {
				LCWLogger.logQueue <- LCWLogger.wrapMessage(DebugLevel, nil, message...)
			}
		} else {
			LCWLogger.setLogger.Debug(message...)
		}
	}
}

//Verbose logs a message at level Verbose on the standard logger.
func (LCWLogger *fullLCWLogger) Verbose(message ...interface{}) {
	if LCWLogger.highestLevel >= VerboseLevel {
		if LCWLogger.setLogger == nil {
			if LCWLogger.forceFlush.Load() {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				LCWLogger.logQueue <- LCWLogger.wrapMessage(VerboseLevel, wg, message...)
				wg.Wait()
			} else {
				LCWLogger.logQueue <- LCWLogger.wrapMessage(VerboseLevel, nil, message...)
			}
		} else {
			LCWLogger.setLogger.Debug(message...)
		}
	}
}

// Warn logs a message at level Warn on the standard logger.
func (LCWLogger *fullLCWLogger) Warn(message ...interface{}) {
	if LCWLogger.highestLevel >= WarnLevel {
		if LCWLogger.setLogger == nil {
			if LCWLogger.forceFlush.Load() {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				LCWLogger.logQueue <- LCWLogger.wrapMessage(WarnLevel, wg, message...)
				wg.Wait()
			} else {
				LCWLogger.logQueue <- LCWLogger.wrapMessage(WarnLevel, nil, message...)
			}
		} else {
			LCWLogger.setLogger.Warn(message...)
		}
	}
}

// Trace logs a message at level Warn on the standard logger.
func (LCWLogger *fullLCWLogger) Trace(message ...interface{}) {
	if LCWLogger.highestLevel >= TraceLevel {
		if LCWLogger.setLogger == nil {
			if LCWLogger.forceFlush.Load() {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				LCWLogger.logQueue <- LCWLogger.wrapMessage(TraceLevel, wg, message...)
				wg.Wait()
			} else {
				LCWLogger.logQueue <- LCWLogger.wrapMessage(TraceLevel, nil, message...)
			}
		} else {
			LCWLogger.setLogger.Debug(message...)
		}
	}
}

// Info logs a message at level Info on the standard logger.
func (LCWLogger *fullLCWLogger) Info(message ...interface{}) {
	if LCWLogger.highestLevel >= InfoLevel {
		if LCWLogger.setLogger == nil {

			if LCWLogger.forceFlush.Load() {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				LCWLogger.logQueue <- LCWLogger.wrapMessage(InfoLevel, wg, message...)
				wg.Wait()
			} else {
				LCWLogger.logQueue <- LCWLogger.wrapMessage(InfoLevel, nil, message...)
			}

		} else {
			LCWLogger.setLogger.Debug(message...)
		}
	}
}

// Fatal logs a message at level Fatal on the standard logger then the process will exit with status set to 1.
func (LCWLogger *fullLCWLogger) Fatal(message ...interface{}) {
	if LCWLogger.highestLevel >= FatalLevel {
		if LCWLogger.setLogger == nil {
			wg := &sync.WaitGroup{}
			wg.Add(1)
			LCWLogger.logQueue <- LCWLogger.wrapMessage(FatalLevel, wg, message...)
			wg.Wait()
		} else {
			LCWLogger.setLogger.Fatal(message...)
		}
		os.Exit(5)
	}
}

type lcwPrefixLogger struct {
	LCWLogger *fullLCWLogger
	prefix    string
}

func (spl *lcwPrefixLogger) prefixLog(i ...interface{}) []interface{} {
	s := fmt.Sprintf("%v", i[0])
	var sb strings.Builder
	sb.WriteString(spl.prefix)
	sb.WriteString(":")
	sb.WriteString(s)
	i[0] = sb.String()
	return i
}
func (spl *lcwPrefixLogger) Trace(i ...interface{}) {
	if spl.LCWLogger.GetLogLevel() >= TraceLevel {
		spl.LCWLogger.Trace(spl.prefixLog(i...)...)
	}
}
func (spl *lcwPrefixLogger) Debug(i ...interface{}) {
	if spl.LCWLogger.GetLogLevel() >= DebugLevel {
		spl.LCWLogger.Debug(spl.prefixLog(i...)...)
	}
}
func (spl *lcwPrefixLogger) Verbose(i ...interface{}) {
	if spl.LCWLogger.GetLogLevel() >= VerboseLevel {
		spl.LCWLogger.Verbose(spl.prefixLog(i...)...)
	}
}
func (spl *lcwPrefixLogger) Info(i ...interface{}) {
	if spl.LCWLogger.GetLogLevel() >= InfoLevel {
		spl.LCWLogger.Info(spl.prefixLog(i...)...)
	}
}
func (spl *lcwPrefixLogger) Warn(i ...interface{}) {
	if spl.LCWLogger.GetLogLevel() >= WarnLevel {
		spl.LCWLogger.Warn(spl.prefixLog(i...)...)
	}
}
func (spl *lcwPrefixLogger) Fatal(i ...interface{}) {
	if spl.LCWLogger.GetLogLevel() >= FatalLevel {
		spl.LCWLogger.Fatal(spl.prefixLog(i...)...)
	}
}
func (spl *lcwPrefixLogger) GetLogLevel() Level {
	return spl.LCWLogger.GetLogLevel()
}
