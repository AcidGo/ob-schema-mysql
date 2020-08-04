package main

import (
    "errors"
    "flag"
    "fmt"
    "log"
    "io"
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
    RE_USE_BLOOM_FILETER = " USE_BLOOM_FILTER\\s*=\\s*\\S+"
    RE_PCTFREE = "PCTFREE\\s*=\\s*\\d+"
    RE_TABLET_SIZE = " TABLET_SIZE\\s*=\\s*\\d+"
    RE_AUTO_INCREMENT_N = "(?im) AUTO_INCREMENT\\s*=\\s*\\d+"
    RE_AUTO_INCREMENT = "(?im)AUTO_INCREMENT\\s*"

    RE_TABLE_START = "^\\s*CREATE\\s+TABLE\\s+.*?$"
    RE_TABLE_END = "^\\s*^(?!/\\*).*;\\s*$"

    RE_CALC_1 = "(?im)`\\S+`\\s*(TINYINT|YEAR)(\\(|\\s).*?$"
    RE_CALC_2 = "(?im)`\\S+`\\s*(SMALLINT)(\\(|\\s).*?$"
    RE_CALC_3 = "(?im)`\\S+`\\s*(MEDIUMINT|DATE|TIME)(\\(|\\s).*?$"
    RE_CALC_4 = "(?im)`\\S+`\\s*(INT|INTEGER|FLOAT|TIMESTAMP)(\\(|\\s).*?$"
    RE_CALC_8 = "(?im)`\\S+`\\s*(BIGINT|DOUBLE|DATETIME|DECIMAL)(\\(|\\s).*?$"
    RE_CALC_N = "(?im)`\\S+`\\s*(CHAR|VARCHAR)\\s*\\(\\s*(\\d+)\\s*\\).*?$"
    RE_CALC_M = "(?im)`\\S+`\\s*(TINYBLOB|TINYTEXT|BLOB|TEXT|MEDIUMBLOB|MEDIUMTEXT|LONGBLOB|LONGTEXT)\\s*.*?$"
    RE_CALC_VARCHAR_11      = "(?im)\\s*VARCHAR\\s*\\(\\s*(\\d{2})\\s*\\)"
    RE_CALC_VARCHAR_111     = "(?im)\\s*VARCHAR\\s*\\(\\s*(\\d{3})\\s*\\)"
    RE_CALC_VARCHAR_1111    = "(?im)\\s*VARCHAR\\s*\\(\\s*(\\d{4})\\s*\\)"
    RE_CALC_VARCHAR_11111   = "(?im)\\s*VARCHAR\\s*\\(\\s*(\\d{5,})\\s*\\)"

    RE_KEY_PRIMARY = "(?m)\\s*PRIMARY\\sKEY\\s*.*?$"
    RE_KEY_FOREIGN = "(?m)\\s*.*?FOREIGN KEY.*?$"
    RE_KEY_KEY = "(?m)\\s*KEY(\\s|\\().*?$"
    RE_KEY_UNIQUE = "(?m)\\s*UNIQUE KEY\\s*.*?$"

    RE_TABLE_COMMA_LIMIT = "(,)(\\n*\\s*\\).*?;)"

    MYSQL_LINE_MAX_SIZE = 65500
    MYSQL_CHAR_BYTE = 4
    MYSQL_VARCHAR_BYTE = 4

    APP_ORIGINAL_FOLDER = "_ob-schema-mysql_"
    APP_TEMPDIR_PRENAME = "_tempdir_"
    APP_TABLE_SCHEMA_GLOB = "*-schema.sql"
    APP_DATABASE_SCHEMA_GLOB = "*-schema-create.sql"
    APP_SWAP_SUFFIX = "._swap"
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

    termTBDeleteREList  []*regexp.Regexp
    termDBDeleteREMap   map[string]*regexp.Regexp
    keyDeleteREList     []*regexp.Regexp
    commaTBRE           *regexp.Regexp

    appName             string
    appAuthor           string
    appVersion          string
    appGitCommitHash    string
    appBuildTime        string
)

var (
    patternCalc_1   *regexp.Regexp
    patternCalc_2   *regexp.Regexp
    patternCalc_3   *regexp.Regexp
    patternCalc_4   *regexp.Regexp
    patternCalc_8   *regexp.Regexp
    patternCalc_N   *regexp.Regexp
    patternCalc_M   *regexp.Regexp
    patternCalc_VARCHAR_11      *regexp.Regexp
    patternCalc_VARCHAR_111     *regexp.Regexp
    patternCalc_VARCHAR_1111    *regexp.Regexp
    patternCalc_VARCHAR_11111   *regexp.Regexp
)

func flagUsage() {
    fmt.Fprintf(os.Stderr, `ob_schema_mysql:
    version: %s/%s
    author: %s
    gitCommit: %s
    buildTime: %s
Usage: %s [-h] [-d schema-dir] [-r schema-dir]
Options:`, appName, appVersion, appAuthor, appGitCommitHash, appBuildTime, appName)
    fmt.Fprintf(os.Stderr, "\n")
    flag.PrintDefaults()
}

func IsDir(path string) bool {
    s, err := os.Stat(path)
    if err != nil {
        return false
    }
    return s.IsDir()
}

func IsEmptyDir(path string) bool {
    dir, err := ioutil.ReadDir(path)
    return len(dir) == 0 && err == nil
}

func GetFilesFromDir(path string) ([]string, error) {
    var files []string
    pathSep := string(os.PathSeparator)
    fiLst, err := ioutil.ReadDir(path)
    if err != nil {
        return nil, err
    }

    for _, fi := range fiLst {
        if !fi.IsDir() {
            files = append(files, path+pathSep+fi.Name())
        }
    }
    return files, nil
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

func SwapDirFiles(dirS string, dirD string) error {
    filesS, err := GetFilesFromDir(dirS)
    if err != nil {
        return err
    }
    filesD, err := GetFilesFromDir(dirD)
    if err != nil {
        return err
    }

    for _, fileD := range filesD {
        baseName := filepath.Base(fileD)
        err = MoveFile(fileD, path.Join(dirS, baseName+APP_SWAP_SUFFIX))
        if err != nil {
            return err
        }
    }
    for _, fileS := range filesS {
        baseName := filepath.Base(fileS)
        err = MoveFile(fileS, path.Join(dirD, baseName))
        if err != nil {
            return err
        }
    }
    for _, fileD := range filesD {
        baseName := filepath.Base(fileD)
        err = MoveFile(path.Join(dirS, baseName+APP_SWAP_SUFFIX), path.Join(dirS, baseName))
        if err != nil {
            return err
        }
    }

    return nil
}

func ConvTBSchemaFile(smFileS string, smFileD string) error {
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

    schemaS, err = deleteTBTerm(schemaS)
    if err != nil {
        return err
    }
    schemaS, err = deleteTBKey(schemaS)
    if err != nil {
        return err
    }
    lineSize, err := calcTBLineSize(schemaS)
    if err != nil {
        return err
    }
    if lineSize > MYSQL_LINE_MAX_SIZE {
        log.Printf("the table schema should be conver the line size, it is %d bytes\n", lineSize)
        schemaS, err = convTBSchemaLineSize(schemaS, MYSQL_LINE_MAX_SIZE)
        if err != nil {
            return err
        }
    }
    schemaS, err = pruneTBComma(schemaS)
    if err != nil {
        return err
    }

    err = ioutil.WriteFile(smFileD, []byte(schemaS), 0666)
    if err != nil {
        return err
    }

    return nil
}

func ConvDBSchemaFile(smFileS string, smFileD string) error {
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

    schemaS, err = deleteDBTerm(schemaS)
    if err != nil {
        return err
    }

    err = ioutil.WriteFile(smFileD, []byte(schemaS), 0666)
    if err != nil {
        return err
    }

    return nil
}

func deleteTBTerm(schema string) (string, error) {
    for _, reV := range termTBDeleteREList {
        schema = reV.ReplaceAllString(schema, "")
    }
    return schema, nil
}

func deleteDBTerm(schema string) (string, error) {
    for _, reV := range termDBDeleteREMap {
        schema = reV.ReplaceAllString(schema, "")
    }
    return schema, nil
}

func deleteTBKey(schema string) (string, error) {
    for _, reV := range keyDeleteREList {
        schema = reV.ReplaceAllString(schema, "")
    }
    return schema, nil
}

func pruneTBComma(schema string) (string, error) {
    schema = commaTBRE.ReplaceAllString(schema, "$2")
    return schema, nil
}

func calcTBLineSize(schema string) (int, error) {
    lineSize := 0
    lineSize = lineSize + len(patternCalc_1.FindAllStringIndex(schema, -1))*1
    lineSize = lineSize + len(patternCalc_2.FindAllStringIndex(schema, -1))*2
    lineSize = lineSize + len(patternCalc_3.FindAllStringIndex(schema, -1))*3
    lineSize = lineSize + len(patternCalc_4.FindAllStringIndex(schema, -1))*4
    lineSize = lineSize + len(patternCalc_8.FindAllStringIndex(schema, -1))*8

    // for variable length field
    _tmp := patternCalc_N.FindAllStringSubmatch(schema, -1)
    for _, val := range _tmp {
        switch strings.ToUpper(val[1]) {
        case "CHAR":
            _t, err := strconv.Atoi(val[2])
            if err != nil {
                return 0, err
            }
            lineSize = lineSize + _t*MYSQL_CHAR_BYTE
        case "VARCHAR":
            _t, err := strconv.Atoi(val[2])
            if err != nil {
                return 0, err
            }
            lineSize = lineSize + _t*MYSQL_VARCHAR_BYTE
        default:
            log.Fatalf("not case the variable length field for %s", val[1])
        }
    }

    return lineSize, nil
}

func convTBSchemaLineSize(schema string, maxSize int) (string, error) {
    schema = patternCalc_VARCHAR_11111.ReplaceAllString(schema, " TEXT")
    _lineSize, err := calcTBLineSize(schema)
    if err != nil {
        return "", err
    }
    if _lineSize < maxSize {
        return schema, nil
    }

    schema = patternCalc_VARCHAR_1111.ReplaceAllString(schema, " TEXT")
    _lineSize, err = calcTBLineSize(schema)
    if err != nil {
        return "", err
    }
    if _lineSize < maxSize {
        return schema, nil
    }

    schema = patternCalc_VARCHAR_111.ReplaceAllString(schema, " TEXT")
    _lineSize, err = calcTBLineSize(schema)
    if err != nil {
        return "", err
    }
    if _lineSize < maxSize {
        return schema, nil
    }

    schema = patternCalc_VARCHAR_11.ReplaceAllString(schema, " TEXT")
    _lineSize, err = calcTBLineSize(schema)
    if err != nil {
        return "", err
    }
    if _lineSize < maxSize {
        return schema, nil
    }

    return schema, errors.New("cannot conver the schema line size")
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
    workPath = flag.Args()[0]
    if (flagDeal && flagRecovery) || (!flagDeal && !flagRecovery) {
        log.Println("the -d <flagDeal> and -r <flagRecovery> are conflict")
        os.Exit(APP_EC_FLAGERROR)
    }
    if !IsDir(workPath) {
        fmt.Fprintf(os.Stderr, "the work folder path is invalid\n")
        os.Exit(APP_EC_WORKPATH_NOT_FOUND)
    }
}

func initRE() {
    // for ROW_FORMAT
    termTBDeleteREList = append(termTBDeleteREList, regexp.MustCompile(RE_ROW_FORMAT))
    // for COMPRESSION
    termTBDeleteREList = append(termTBDeleteREList, regexp.MustCompile(RE_COMPRESSION))
    // for REPLICA_NUM
    termTBDeleteREList = append(termTBDeleteREList, regexp.MustCompile(RE_REPLICA_NUM))
    // for PRIMARY_ZONE
    termTBDeleteREList = append(termTBDeleteREList, regexp.MustCompile(RE_PRIMARY_ZONE))
    // for BLOCK_SIZE
    termTBDeleteREList = append(termTBDeleteREList, regexp.MustCompile(RE_BLOCK_SIZE))
    // for USE_BLOOM_FILETER
    termTBDeleteREList = append(termTBDeleteREList, regexp.MustCompile(RE_USE_BLOOM_FILETER))
    // for PCTFREE
    termTBDeleteREList = append(termTBDeleteREList, regexp.MustCompile(RE_PCTFREE))
    // for TABLE_SIZE
    termTBDeleteREList = append(termTBDeleteREList, regexp.MustCompile(RE_TABLET_SIZE))
    // for AUTO_INCREMENT col
    termTBDeleteREList = append(termTBDeleteREList, regexp.MustCompile(RE_AUTO_INCREMENT_N))
    // for AUTO_INCREMENT
    termTBDeleteREList = append(termTBDeleteREList, regexp.MustCompile(RE_AUTO_INCREMENT))

    // for PRIMARY KEY
    keyDeleteREList = append(keyDeleteREList, regexp.MustCompile(RE_KEY_PRIMARY))
    // for FOREIGN KEY
    keyDeleteREList = append(keyDeleteREList, regexp.MustCompile(RE_KEY_FOREIGN))
    // for UNIQUE KEY
    keyDeleteREList = append(keyDeleteREList, regexp.MustCompile(RE_KEY_UNIQUE))
    // for INDEX KEY
    keyDeleteREList = append(keyDeleteREList, regexp.MustCompile(RE_KEY_KEY))


    termDBDeleteREMap = make(map[string]*regexp.Regexp)
    // for REPLICA_NUM
    termDBDeleteREMap["REPLICA_NUM"] = regexp.MustCompile(RE_REPLICA_NUM)
    // for PRIMARY_ZONE
    termDBDeleteREMap["PRIMARY_ZONE"] = regexp.MustCompile(RE_PRIMARY_ZONE)

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
    patternCalc_VARCHAR_11111   = regexp.MustCompile(RE_CALC_VARCHAR_11111)

    commaTBRE = regexp.MustCompile(RE_TABLE_COMMA_LIMIT)
}

func init() {
    initFlag()
    initRE()
}


func main() {
    appName = ""
    appVersion = ""
    appAuthor = ""
    appGitCommitHash = ""
    appBuildTime = ""

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
        recoverPath := path.Join(workPath, APP_ORIGINAL_FOLDER)
        if IsDir(recoverPath) {
            if !IsEmptyDir(recoverPath) {
                log.Fatal("the recover dir must be empty")
            }
        } else {
            err := os.Mkdir(recoverPath, os.ModePerm)
            if err != nil {
                log.Fatal(err)
            }
        }

        tempDir, err := ioutil.TempDir(workPath, APP_TEMPDIR_PRENAME)
        if err != nil {
            log.Fatal(err)
        }
        tempPath := path.Join(workPath, filepath.Base(tempDir))

        // do for table schema
        log.Println("start doing for table schema")
        tbSMFiles, err := filepath.Glob(path.Join(workPath, APP_TABLE_SCHEMA_GLOB))
        if err != nil {
            log.Fatal(err)
        }
        for _, tbSMFile := range tbSMFiles {
            tbSMFileD := path.Join(tempPath, filepath.Base(tbSMFile))
            log.Println(tbSMFileD)
            err := ConvTBSchemaFile(tbSMFile, tbSMFileD)
            if err != nil {
                log.Fatal(err)
            }
        }

        // do for database schema
        log.Println("start doing for database schema")
        dbSMFiles, err := filepath.Glob(path.Join(workPath, APP_DATABASE_SCHEMA_GLOB))
        if err != nil {
            log.Fatal(err)
        }
        for _, dbSMFile := range dbSMFiles {
            dbSMFileD := path.Join(tempPath, filepath.Base(dbSMFile))
            log.Println(dbSMFileD)
            err := ConvDBSchemaFile(dbSMFile, dbSMFileD)
            if err != nil {
                log.Fatal(err)
            }
        }

        // move the original schema files to recovery dir
        log.Println("start moving the original schema files to recovery dir")
        for _, tbSMFile := range tbSMFiles {
            baseName := filepath.Base(tbSMFile)
            fileD := path.Join(recoverPath, baseName)
            log.Printf("move %s to %s", tbSMFile, fileD)
            err := MoveFile(tbSMFile, fileD)
            if err != nil {
                log.Fatal(err)
            }
        }
        for _, dbSMFile := range dbSMFiles {
            baseName := filepath.Base(dbSMFile)
            fileD := path.Join(recoverPath, baseName)
            log.Printf("move %s to %s", dbSMFile, fileD)
            err := MoveFile(dbSMFile, fileD)
            if err != nil {
                log.Fatal(err)
            }
        }

        // move the cooked schema files to original dir
        log.Println("start moving the cooked schema files to original dir")
        for _, tbSMFile := range tbSMFiles {
            baseName := filepath.Base(tbSMFile)
            fileS := path.Join(tempDir, baseName)
            err := MoveFile(fileS, tbSMFile)
            log.Printf("move %s to %s", fileS, tbSMFile)
            if err != nil {
                log.Fatal(err)
            }
        }
        for _, dbSMFile := range dbSMFiles {
            baseName := filepath.Base(dbSMFile)
            fileS := path.Join(tempDir, baseName)
            err := MoveFile(fileS, dbSMFile)
            log.Printf("move %s to %s", fileS, dbSMFile)
            if err != nil {
                log.Fatal(err)
            }
        }

        // remove tempdir
        log.Println("start removing tempdir")
        if IsEmptyDir(tempDir) {
            err := os.Remove(tempDir)
            if err != nil {
                log.Fatal(err)
            }
        } else {
            log.Printf("the tempdir %s is not empty, not remove it, please check", tempDir)
        }

        log.Println("done for all jobs")
        os.Exit(APP_EC_SUCCESS)
    }
}