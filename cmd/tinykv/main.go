// Command tinykv runs a tinykv server or talks to one over HTTP.
//
//	tinykv serve [--dir data] [--addr :7700] [--fsync always|never|100ms]
//	tinykv get     [--addr :7700] <key>
//	tinykv set     [--addr :7700] [--ttl 30s] <key> <value>
//	tinykv del     [--addr :7700] <key>
//	tinykv keys    [--addr :7700] [prefix]
//	tinykv stats   [--addr :7700]
//	tinykv compact [--addr :7700]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sumit-do/tinykv"
	"github.com/sumit-do/tinykv/internal/server"
)

const defaultAddr = ":7700"

// version is stamped at build time with -ldflags "-X main.version=v1.2.3".
var version = "dev"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		usage(stderr)
		return 2
	}
	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "version", "-version", "--version":
		fmt.Fprintln(stdout, "tinykv", version)
		return 0
	case "serve":
		err = cmdServe(rest, stderr)
	case "get", "set", "del", "keys", "stats", "compact":
		err = cmdClient(cmd, rest, stdout)
	default:
		fmt.Fprintf(stderr, "tinykv: unknown command %q\n\n", cmd)
		usage(stderr)
		return 2
	}
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 2
		}
		if errors.Is(err, errUsage) {
			fmt.Fprintf(stderr, "tinykv %s: %v\n", cmd, err)
			return 2
		}
		fmt.Fprintf(stderr, "tinykv %s: %v\n", cmd, err)
		if errors.Is(err, errNotFound) {
			return 3
		}
		return 1
	}
	return 0
}

var errUsage = errors.New("usage error")

func usage(w io.Writer) {
	fmt.Fprint(w, `tinykv - a tiny persistent key-value store

Usage:
  tinykv serve   [--dir data] [--addr :7700] [--fsync always|never|<interval>]
  tinykv get     [--addr :7700] <key>
  tinykv set     [--addr :7700] [--ttl 30s] <key> <value>
  tinykv del     [--addr :7700] <key>
  tinykv keys    [--addr :7700] [prefix]
  tinykv stats   [--addr :7700]
  tinykv compact [--addr :7700]
  tinykv version

Flags must come before positional arguments.
`)
}

// parseSync turns the --fsync flag value into engine options.
func parseSync(v string) (tinykv.SyncMode, time.Duration, error) {
	switch strings.ToLower(v) {
	case "always":
		return tinykv.SyncAlways, 0, nil
	case "never":
		return tinykv.SyncNever, 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, 0, fmt.Errorf("%w: --fsync must be always, never or a positive duration such as 100ms", errUsage)
	}
	return tinykv.SyncInterval, d, nil
}

func cmdServe(args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "data", "data directory")
	addr := fs.String("addr", defaultAddr, "listen address")
	fsync := fs.String("fsync", "always", "fsync policy: always, never, or an interval like 100ms")
	sweep := fs.Duration("sweep", time.Minute, "how often expired keys are purged from memory")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn, error")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%w: unexpected arguments %v", errUsage, fs.Args())
	}
	mode, interval, err := parseSync(*fsync)
	if err != nil {
		return err
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("%w: invalid --log-level %q", errUsage, *logLevel)
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: lvl}))

	db, err := tinykv.Open(*dir, tinykv.Options{
		Sync:          mode,
		SyncInterval:  interval,
		SweepInterval: *sweep,
		Logger:        logger,
	})
	if err != nil {
		return err
	}
	defer db.Close()
	st, _ := db.Stats()
	logger.Info("opened store", "dir", *dir, "keys", st.Keys, "file_size", st.FileSize, "fsync", *fsync)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := server.New(db, logger)
	if err := srv.ListenAndServe(ctx, *addr); err != nil {
		return err
	}
	logger.Info("closing store")
	return db.Close()
}

func cmdClient(cmd string, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	addr := fs.String("addr", envOr("TINYKV_ADDR", defaultAddr), "server address (host:port or URL)")
	ttl := fs.Duration("ttl", 0, "time to live for set, e.g. 30s (0 = never expires)")
	timeout := fs.Duration("timeout", 10*time.Second, "request timeout")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %v", errUsage, err)
	}
	c := newClient(*addr, *timeout)
	pos := fs.Args()
	need := func(n int, what string) error {
		if len(pos) != n {
			return fmt.Errorf("%w: %s %s", errUsage, cmd, what)
		}
		return nil
	}
	switch cmd {
	case "get":
		if err := need(1, "<key>"); err != nil {
			return err
		}
		v, err := c.get(pos[0])
		if err != nil {
			return err
		}
		_, err = stdout.Write(v)
		if err == nil && len(v) > 0 && v[len(v)-1] != '\n' {
			fmt.Fprintln(stdout)
		}
		return err
	case "set":
		if err := need(2, "<key> <value>"); err != nil {
			return err
		}
		return c.set(pos[0], []byte(pos[1]), *ttl)
	case "del":
		if err := need(1, "<key>"); err != nil {
			return err
		}
		return c.del(pos[0])
	case "keys":
		if len(pos) > 1 {
			return need(1, "[prefix]")
		}
		prefix := ""
		if len(pos) == 1 {
			prefix = pos[0]
		}
		keys, err := c.keys(prefix)
		if err != nil {
			return err
		}
		for _, k := range keys {
			fmt.Fprintln(stdout, k)
		}
		return nil
	case "stats":
		st, err := c.stats()
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "keys:       %d\nlive_bytes: %d\ndead_bytes: %d\nfile_size:  %d\n",
			st.Keys, st.LiveBytes, st.DeadBytes, st.FileSize)
		return nil
	case "compact":
		before, after, err := c.compact()
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "compacted: %d -> %d bytes (reclaimed %d)\n",
			before.FileSize, after.FileSize, before.FileSize-after.FileSize)
		return nil
	}
	return fmt.Errorf("%w: unknown command %q", errUsage, cmd)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
