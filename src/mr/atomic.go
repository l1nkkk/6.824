package mr

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// why：map-reduce 在某个任务阻塞的时候，会重新下发同样的任务给其他节点，这个时候可能出现多个worker在写同样的文件，导致异常

// atomicWriteFile 需要保证 map 任务和 reduce 任务生成文件时的原子性，从而避免某些异常情况导致文件受损，
// 使得之后的任务无法执行下去的 bug。具体方法就是先生成一个临时文件再利用系统调用 OS.Rename 来完成原子性替换，这样即可保证写文件的原子性
func atomicWriteFile(filename string, r io.Reader) (err error) {
	// 先生成一个临时文件再利用系统调用 OS.Rename 来完成原子性替换
	// write to a temp file first, then we'll atomically replace the target file
	// with the temp file.
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}

	// f, err := ioutil.TempFile("/home/l1nkkk", "testtmp")
	// 生成的临时文件名：/home/l1nkkk/testtmp506971204
	f, err := ioutil.TempFile(dir, file)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}
	defer func() {
		if err != nil {
			// Don't leave the temp file lying around on error.
			_ = os.Remove(f.Name()) // yes, ignore the error, not much we can do about it.
		}
	}()
	// ensure we always close f. Note that this does not conflict with  the
	// close below, as close is idempotent.
	defer f.Close()
	name := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("cannot write data to tempfile %q: %v", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("can't close tempfile %q: %v", name, err)
	}

	// get the file mode from the original file and use that for the replacement
	// file, too.
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		// no original file
	} else if err != nil {
		return err
	} else {
		if err := os.Chmod(name, info.Mode()); err != nil {
			return fmt.Errorf("can't set filemode on tempfile %q: %v", name, err)
		}
	}
	if err := os.Rename(name, filename); err != nil {
		return fmt.Errorf("cannot replace %q with tempfile %q: %v", filename, name, err)
	}
	return nil
}
