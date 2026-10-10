package workflows

import "runtime"

func runtimeGoroutines() int { return runtime.NumGoroutine() }
