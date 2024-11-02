package main

import (
	"bytes"
	"os"
	"testing"
)

func TestCreate(t *testing.T) {
	tests := []struct {
		name       string
		args       args
		wantErr    bool
		wantOutput string
	}{
		{
			name: "no input files",
			args: args{
				Create:  true,
				Archive: "test.txt",
				Files:   []string{},
			},
			wantErr:    false,
			wantOutput: "creating test.txt from []\nwrote 0 files\n",
		},
		{
			name: "one input file",
			args: args{
				Create:  true,
				Archive: "test.txt",
				Files:   []string{"test_input.txt"},
			},
			wantErr:    false,
			wantOutput: "creating test.txt from [test_input.txt]\nwrote 1 files\n",
		},
		{
			name: "multiple input files",
			args: args{
				Create:  true,
				Archive: "test.txt",
				Files:   []string{"test_input1.txt", "test_input2.txt"},
			},
			wantErr:    false,
			wantOutput: "creating test.txt from [test_input1.txt test_input2.txt]\nwrote 2 files\n",
		},
		{
			name: "non-existent input files",
			args: args{
				Create:  true,
				Archive: "test.txt",
				Files:   []string{"non_existent_file.txt"},
			}, wantErr: false,
			wantOutput: "creating test.txt from [non_existent_file.txt]\nwrote 0 files\n",
		},
		{
			name: "invalid command",
			args: args{
				Create:  false,
				Archive: "test.txt",
				Files:   []string{},
			},
			wantErr:    true,
			wantOutput: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a temporary directory for the input files
			tmpDir, err := os.MkdirTemp("", "lsm2-cli-test")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tmpDir)
			os.Chdir(tmpDir)

			// Create the input files
			for _, file := range tc.args.Files {
				if file == "non_existent_file.txt" {
					continue
				}
				if err := os.WriteFile(file, []byte(file), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			// Create a buffer to capture the output
			var output bytes.Buffer

			// Call the create function
			err = run(console{Stdout: &output}, tc.args)

			// Check the output
			if output.String() != tc.wantOutput {
				t.Errorf("run() output = %q, want %q", output.String(), tc.wantOutput)
			}

			// Check for errors
			if tc.wantErr && err == nil {
				t.Errorf("run() did not return an error")
			}
		})
	}
}
