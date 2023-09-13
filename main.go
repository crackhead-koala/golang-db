package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Row struct
// TODO(ssuvorov): limit username and email?
type Row struct {
	id       uint32
	username string
	email    string
}

func NewRow() Row {
	var row Row
	row.id = 0
	row.username = ""
	row.email = ""
	return row
}

func (row *Row) isEmpty() bool {
	return row.id == 0 && row.username == "" && row.email == ""
}

// PrepareResult enum
func doMetaCommand(command string) error {
	switch command {
	case ".exit":
		fmt.Println("bye.")
		os.Exit(0)
		return nil
	default:
		return errors.New("UNRECOGNIZED_META_COMMAND")
	}
}

// Table struct
// TODO(ssuvorov): figure out how to do this in Go...
// #define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
// const uint32_t ID_SIZE = size_of_attribute(Row, id);
// const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
// const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
// const uint32_t ID_OFFSET = 0;
// const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
// const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
// const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

type Table struct {
	rows []Row
	// maxFieldLen int
}

// func NewTable() Table {
// 	var table Table
// 	return table
// }

func (table *Table) insertRow(row Row) {
	table.rows = append(table.rows, row)
}

func (table *Table) selectAllRows() []Row {
	return table.rows
}

// StatementType enum
type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

type Statement struct {
	statenemtType StatementType
	rowToInsert   Row
}

func parseCommand(command string) (string, uint32, string, string, error) {
	var parsedCommand, parsedArgs string
	for idx, char := range command {
		if char == 32 { // [space] rune
			parsedCommand, parsedArgs = command[:idx], command[(idx+1):]
			break
		}
	}

	if parsedArgs == "" {
		return command, 0, "", "", nil
	}

	args := strings.Split(parsedArgs, ",")
	if len(args) != 3 {
		return "", 0, "", "", errors.New("COMMAND_ARGUMENTS_PARSE_ERROR")
	}

	id, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return "", 0, "", "", errors.New("COMMAND_ARGUMENTS_PARSE_ERROR")
	}

	username := strings.TrimSpace(args[1])
	email := strings.TrimSpace(args[2])

	return parsedCommand, uint32(id), username, email, nil
}

func (statement *Statement) prepare(command string) error {
	parsedCommand, id, username, email, err := parseCommand(command)
	if err != nil {
		return err
	}

	switch parsedCommand {
	case "insert":
		if id == 0 && username == "" && email == "" {
			return errors.New("COMMAND_ARGUMENTS_PARSE_ERROR")
		}
		statement.statenemtType = STATEMENT_INSERT
		statement.rowToInsert = Row{id: id, username: username, email: email}
		return nil
	case "select":
		statement.statenemtType = STATEMENT_SELECT
		return nil
	}

	return errors.New("UNRECOGNIZED_STATEMENT")
}

func (statement *Statement) executeInsert(table *Table) {
	table.insertRow(statement.rowToInsert)
}

type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func countDigits[T Integer](n T) int {
	digits := 0
	for n != 0 {
		n /= 10
		digits++
	}
	return digits
}

func (statement *Statement) executeSelectAll(table *Table) {
	if table.rows == nil {
		fmt.Println("Empty table")
	} else {
		rows := table.selectAllRows()
		maxIdlen, maxUsernameLen, maxEmailLen := 2, 8, 5
		for _, row := range rows {
			if idLen := countDigits(row.id); idLen > maxIdlen {
				maxIdlen = idLen
			}
			if usernameLen := len(row.username); usernameLen > maxUsernameLen {
				maxUsernameLen = usernameLen
			}
			if emailLen := len(row.email); emailLen > maxEmailLen {
				maxEmailLen = emailLen
			}
		}

		fmt.Printf(
			"┌%s %s ┬ %s %s┬ %s %s┐\n",
			strings.Repeat("─", maxIdlen-2), "id",
			"username", strings.Repeat("─", maxUsernameLen-8),
			"email", strings.Repeat("─", maxEmailLen-5),
		)

		for _, row := range rows {
			fmt.Printf(
				"│ %*d │ %-*s │ %-*s │\n",
				maxIdlen, row.id,
				maxUsernameLen, row.username,
				maxEmailLen, row.email,
			)
		}

		fmt.Printf(
			"└─%s─┴─%s─┴─%s─┘\n",
			strings.Repeat("─", maxIdlen),
			strings.Repeat("─", maxUsernameLen),
			strings.Repeat("─", maxEmailLen),
		)
	}
}

func (statement *Statement) execute(table *Table) {
	switch statement.statenemtType {
	case STATEMENT_INSERT:
		statement.executeInsert(table)
	case STATEMENT_SELECT:
		statement.executeSelectAll(table)

	}
}

// REPL
func print_prompt() {
	fmt.Print("db> ")
}

func main() {
	var table Table
	for {
		print_prompt()

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()

		err := scanner.Err()
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}

		command := scanner.Text()

		// if starts with a dot, then it's a meta-command
		if command[0] == '.' {
			err := doMetaCommand(command)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
		}

		// otherwise, it's an "SQL"-statement
		var statement Statement
		err = statement.prepare(command)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}

		statement.execute(&table)
	}
}
