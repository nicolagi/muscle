// The config package encapsulates configuration for all muscle
// commands (muscle, musclefs, snapshotsfs).
//
// All muscle components are expected to store logs, caches, and any
// runtime information within a dedicated base directory. When loading
// the configuration, the first and only argument is the path to the
// base directory rather than the path to the configuration file. The
// designated directory is expected to contain a JSON file called
// 'config' that corresponds to the C struct of this package.  Many
// paths are derived from the base directory and exposed as methods of
// C, e.g., log file paths, cache directory path, staging area, etc.
package config
