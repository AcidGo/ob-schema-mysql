package main

import (
    "errors"
    "flag"
    "fmt"
    "log"
    "io/ioutil"
    "os"
    "path"
    "path/filepath"
    "regexp"
    "strconv"
    "strings"
)

const (
    RE_ROW_FORMAT = " ROW_FORMAT\\s*=\\s*\\w+"
    RE_COMPRESSION = " COMPRESSION\\s*=\\s*'\\S+'"
    RE_REPLICA_NUM = " REPLICA_NUM\\s*=\\s*\\d+"
    RE_PRIMARY_ZONE = " PRIMARY_ZONE\\s*=\\s*'\\S+'"
    RE_BLOCK_SIZE = " BLOCK_SIZE\\s*=?\\s*\\d+"
    RE_USE_BLOOM_FILETER = "PCTFREE\\s*=\\s*\\d"

    RE_TABLE_START = "^\\s*CREATE\s+TABLE\\s+.*?$"
    RE_TABLE_END = "^\\s*^(?!/\\*).*;\\s*$"

    RE_CALC_1 = "`\\S+`\\s*(TINYINT|YEAR)(\\(|\\s).*?$"
    RE_CALC_2 = "`\\S+`\\s*(SMALLINT)(\\(|\\s).*?$"
    RE_CALC_3 = "`\\S+`\\s*(MEDIUMINT|DATE|TIME)(\\(|\\s).*?$"
    RE_CALC_4 = "`\\S+`\\s*(INT|INTEGER|FLOAT|TIMESTAMP)(\\(|\\s).*?$"
    RE_CALC_8 = "`\\S+`\\s*(BIGINT|DOUBLE|DATETIME|DECIMAL)(\\(|\\s).*?$"
    RE_CALC_N = "`\\S+`\\s*(CHAR|VARCHAR)\\s*\\(\\s*(\\d+)\\s*\\).*?$"
    RE_CALC_M = "`\\S+`\\s*(TINYBLOB|TINYTEXT|BLOB|TEXT|MEDIUMBLOB|MEDIUMTEXT|LONGBLOB|LONGTEXT)\\s*.*?$"
    RE_CALC_VARCHAR_11 = "\\s*VARCHAR\\s*\\(\\s*(\\d\\d)\\s*\\)"
    RE_CALC_VARCHAR_111 = "`\\s*VARCHAR\\s*\\(\\s*(\\d\\d\\d)\\s*\\)"
    RE_CALC_VARCHAR_1111 = "`\\s*VARCHAR\\s*\\(\\s*(\\d\\d\\d\\d)\\s*\\)"

    RE_KEY_PRIMARY = "\\s*PRIMARY KEY\\s*(.*?$"
    RE_KEY_KEY = "\\s*KEY\\s*.*?$"
    RE_KEY_UNIQUE = "\\s*UNIQUE KEY\\s*.*?$"

    MYSQL_LINE_MAX_SIZE = 65535
    MYSQL_CHAR_BYTE = 4
    MYSQL_VARCHAR_BYTE = 4

    APP_ORIGINAL_FOLDER = "_ob-schema-mysql_"
    APP_TEMPDIR_PRENAME = "_tempdir_"
    APP_TABLE_SCHEMA_GLOB = "*-schema.sql"
    APP_DATABASE_SCHEMA_GLOB = "*-schema-create.sql"
)

const (
    // Error Code
    APP_EC_SUCCESS = iota
    APP_EC_HELP
    APP_EC_ARGSNUM
    APP_EC_FLAGERROR
    APP_EC_WORKPATH_NOT_FOUND
)


var (
    flagHelp        bool
    flagDeal        bool
    flagRecovery    bool

    workPath    string

    termDeleteREMap     map[string]*regexp.Regexp
    keyDeleteREMap      map[string]*regexp.Regexp

    appName             string
    appAuthor           string
    appVersion          string
    appGitCommitHash    string
)

var (
    patternCalc_1   *regexp.Regexp
    patternCalc_2
    patternCalc_3
    patternCalc_4
    patternCalc_8
    patternCalc_N
    patternCalc_M
    patternCalc_VARCHAR_11
    patternCalc_VARCHAR_111
    patternCalc_VARCHAR_1111
)

func flagUsage() {
    fmt.Fprintf(os.Stderr, `ob_schema_mysql version: %s/%s,
    author: %s
    gitCommit: %s
Usage: %s [-h] [-d schema-dir] [-r schema-dir]
Options:`, appName, appVersion, appAuthor, appGitCommitHash, appName)

    flag.PrintDefaults()
}

func IsDir(path string) bool {
    s, err := os.Stat(path)
    if err != nil {
        return false
    }
    return s.IsDir()
}

func GetMaxIndex(lst []int) int {
    var idx int
    _max := -99999
    for i, v := range lst {
        if v > _max {
            _max = v
            idx = i
        }
    }
    return idx
}

func MoveFile(sourcePath string, destPath string) error {
    inputFile, err := os.Open(sourcePath)
    if err != nil {
        return fmt.Errorf("Couldn't open source file: %s", err)
    }
    outputFile, err := os.Create(destPath)
    if err != nil {
        inputFile.Close()
        return fmt.Errorf("Couldn't open dest file: %s", err)
    }
    defer outputFile.Close()
    _, err = io.Copy(outputFile, inputFile)
    inputFile.Close()
    if err != nil {
        return fmt.Errorf("Writing to output file failed: %s", err)
    }
    // The copy was successful, so now delete the original file
    err = os.Remove(sourcePath)
    if err != nil {
        return fmt.Errorf("Failed removing original file: %s", err)
    }
    return nil
}

func ConvSchemaFile(smFileS string, smFileD string) error {
    fileS, err := os.Open(smFileS)
    if err != nil {
        return err
    }
    defer fileS.Close()

    dataS, err := ioutil.ReadAll(fileS)
    if err != nil {
        return err
    }
    schemaS := string(dataS)

    schemaS, err = deleteTerm(schemaS)
    if err != nil {
        return err
    }
    schemaS, err = deleteKey(schemaS)
    if err != nil {
        return err
    }
    lineSize := calcLineSize(schemaS)
    if lineSize > MYSQL_LINE_MAX_SIZE {
        schemaS, err = convSchemaLineSize(schemaS, MYSQL_LINE_MAX_SIZE)
        if err != nil {
            return err
        }
    }

    err = ioutil.WriteFile(smFileD, []byte(schemaS), 0666)
    if err != nil {
        return err
    }

    return nil
}

func deleteTerm(schema string) (string, error) {
    for reK, reV := range termDeleteREMap {
        schema = reV.ReplaceAllString(schema, "")
    }
    return schema, nil
}

func deleteKey(schema string) (string, error) {
    for reK, reV := range keyDeleteREMap {
        schema = reV.ReplaceAllString(schema, "")
    }
    return schema, nil
}

func calcLineSize(schema string) int {
    lineSize := 0
    lineSize = lineSize + len(patternCalc_1.FindAllStringIndex(schema), -1)*1
    lineSize = lineSize + len(patternCalc_2.FindAllStringIndex(schema), -1)*2
    lineSize = lineSize + len(patternCalc_3.FindAllStringIndex(schema), -1)*3
    lineSize = lineSize + len(patternCalc_4.FindAllStringIndex(schema), -1)*4
    lineSize = lineSize + len(patternCalc_8.FindAllStringIndex(schema), -1)*8

    // for variable length field
    _tmp := patternCalc_N.FindStringSubmatch(schema)
    for _, val := range _tmp {
        switch strings.ToUpper(val[1]) {
        case "CHAR":
            lineSize = lineSize + strconv.Atoi(val[2])*MYSQL_CHAR_BYTE
        case "VARCHAR": 
            lineSize = lineSize + strconv.Atoi(val[2])*MYSQL_VARCHAR_BYTE
        default:
            log.Fatalf("not case the variable length field for %s", val[1])
        }
    }

    return lineSize
}

func convSchemaLineSize(schema stirng, maxSize int) (string, error) {
    schema := patternCalc_VARCHAR_1111.ReplaceAllString(schema, " TEXT")
    if calcLineSize(schema) < maxSize {
        return schema, nil
    }
    schema := patternCalc_VARCHAR_111.ReplaceAllString(schema, " TEXT")
    if calcLineSize(schema) < maxSize {
        return schema, nil
    }
    schema := patternCalc_VARCHAR_11.ReplaceAllString(schema, " TEXT")
    if calcLineSize(schema) < maxSize {
        return schema, nil
    }
    return "", errors.New("cannot conver the schema line size")
}

func initFlag() {
    flag.BoolVar(&flagHelp, "h", false, "show for help")
    flag.BoolVar(&flagDeal, "d", true, "deal with schema file from mydumper/mydumper_ac")
    flag.BoolVar(&flagRecovery, "r", false, fmt.Sprintf("recover original schema file from %s", APP_ORIGINAL_FOLDER))

    flag.Usage = flagUsage
    flag.Parse()

    // check for flag args
    if flagHelp {
        flag.Usage()
        os.Exit(APP_EC_HELP)
    }
    if flag.NArg() > 1 {
        fmt.Fprintf(os.Stderr, "must input one arg\n")
        os.Exit(APP_EC_ARGSNUM)
    }
    workPath = flag.Arg(1)
    if (flagDeal && flagRecovery) || (!flagDeal && !flagRecovery) {
        log.Println("the -d <flagDeal> and -r <flagRecovery> are conflict")
        os.Exit(APP_EC_FLAGERROR)
    }
    if !IsDir(workPath) {
        fmt.Fprintf(os.Stderr, "the work folder path is invalid")
        os.Exit(APP_EC_WORKPATH_NOT_FOUND)
    }
}

func initRE() {
    termDeleteREMap = make(map[string]*regexp.Regexp)
    // for ROW_FORMAT
    termDeleteREMap["ROW_FORMAT"] = regexp.MustCompile(RE_ROW_FORMAT)
    // for COMPRESSION
    termDeleteREMap["COMPRESSION"] = regexp.MustCompile(RE_COMPRESSION)
    // for REPLICA_NUM
    termDeleteREMap["REPLICA_NUM"] = regexp.MustCompile(RE_REPLICA_NUM)
    // for PRIMARY_ZONE
    termDeleteREMap["PRIMARY_ZONE"] = regexp.MustCompile(RE_PRIMARY_ZONE)
    // for BLOCK_SIZE
    termDeleteREMap["BLOCK_SIZE"] = regexp.MustCompile(RE_BLOCK_SIZE)
    // for PCTFREE
    termDeleteREMap["PCTFREE"] = regexp.MustCompile(RE_PCTFREE)

    keyDeleteREMap = make(map[string]*regexp.Regexp)
    // for PRIMARY KEY
    keyDeleteREMap["PRIMARY"] = regexp.MustCompile(RE_KEY_PRIMARY)
    // for INDEX KEY
    keyDeleteREMap["KEY"] = regexp.MustCompile(RE_KEY_KEY)
    // for UNIQUE KEY
    keyDeleteREMap["UNIQUE"] = regexp.MustCompile(RE_KEY_UNIQUE)

    patternCalc_1       = regexp.MustCompile(RE_CALC_1)
    patternCalc_2       = regexp.MustCompile(RE_CALC_2)
    patternCalc_3       = regexp.MustCompile(RE_CALC_3)
    patternCalc_4       = regexp.MustCompile(RE_CALC_4)
    patternCalc_8       = regexp.MustCompile(RE_CALC_8)
    patternCalc_N       = regexp.MustCompile(RE_CALC_N)
    patternCalc_M       = regexp.MustCompile(RE_CALC_M)
    patternCalc_VARCHAR_11      = regexp.MustCompile(RE_CALC_VARCHAR_11)
    patternCalc_VARCHAR_111     = regexp.MustCompile(RE_CALC_VARCHAR_111)
    patternCalc_VARCHAR_1111    = regexp.MustCompile(RE_CALC_VARCHAR_1111)
}

func init() {
    initFlag()
    initRE()
}


func main() {
    if flagRecovery {
        recoverPath := path.Join(workPath, APP_ORIGINAL_FOLDER)

        // do for table schema
        tbSMFiles, err := filepath.Glob(path.Join(recoverPath, APP_TABLE_SCHEMA_GLOB))
        if err != nil {
            log.Fatal(err)
        }
        for _, tbSMFile := range tbSMFiles {
            baseName := filepath.Base(tbSMFile)
            destPath := path.Join(workPath, baseName)
            err = MoveFile(tbSMFile, destPath)
            if err != nil {
                log.Fatal(err)
            }
            log.Printf("done recover original table schema file: %s", baseName)
        }

        // do for database schema
        dbSMFiles, err := filepath.Glob(path.Join(recoverPath, APP_DATABASE_SCHEMA_GLOB))
        if err != nil {
            log.Fatal(err)
        }
        for _, dbSMFile := range dbSMFiles {
            baseName := filepath.Base(dbSMFile)
            destPath := path.Join(workPath, baseName)
            err = MoveFile(dbSMFile, destPath)
            if err != nil {
                log.Fatal(err)
            }
            log.Printf("done recover original database schema file: %s", baseName)
        }

        log.Println("done for all jobs")
        os.Exit(APP_EC_SUCCESS)
    }

    if flagDeal {
        tempDir, err := ioutil.TempDir(workPath, APP_TEMPDIR_PRENAME)
        if err != nil {
            log.Fatal(err)
        }
        tempPath := path.Join(workPath, tempDir)

        // do for table schema
        tbSMFiles, err := filepath.Glob(path.Join(workPath, APP_TABLE_SCHEMA_GLOB))
        if err != nil {
            log.Fatal(err)
        }
        for _, tbSMFile := range tbSMFiles {
            tbSMFileD := path.Join(tempPath, tbSMFile)
            err := ConvSchemaFile(tbSMFile, tbSMFileD)
            if err != nil {
                log.Fatal(err)
            }
        }
        for _, tbSMFile := range tbSMFiles {
            
        }

        // do for database schema

    }
}