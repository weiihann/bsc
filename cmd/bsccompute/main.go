// Command bsccompute is the benchmarkoor compute-protocol v2 worker for BSC. It
// times production Process plus ValidateState of one sealed Parlia block per
// sample and writes one terminal JSONL result per requested sample.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/log"
)

func main() {
	requestPath := flag.String("request", "", "request JSON file")
	outputPath := flag.String("output", "", "JSONL file receiving one result per requested sample")
	flag.Parse()
	if *requestPath == "" || *outputPath == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: bsccompute --request PATH --output PATH")
		os.Exit(2)
	}
	log.SetDefault(log.NewLogger(log.JSONHandlerWithLevel(os.Stderr, log.LevelWarn)))
	if err := run(*requestPath, *outputPath); err != nil {
		fmt.Fprintln(os.Stderr, "bsccompute:", err)
		os.Exit(1)
	}
}

type activeCase struct {
	id    string
	run   *caseRun
	stage string
	err   error
}

func (a *activeCase) close() {
	if a != nil && a.run != nil {
		a.run.close()
	}
}

func run(requestPath, outputPath string) error {
	raw, err := os.ReadFile(requestPath)
	if err != nil {
		return fmt.Errorf("reading request: %w", err)
	}
	var req request
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return fmt.Errorf("decoding request: %w", err)
	}
	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("creating output: %w", err)
	}
	defer out.Close()
	s := &sink{out: out, req: &req}

	if err := validateRequest(&req); err != nil {
		return s.failAll(stageLoadRequest, err)
	}
	rawWorkload, err := os.ReadFile(req.WorkloadPath)
	if err != nil {
		return s.failAll(stageLoadWorkload, err)
	}
	wl, err := loadWorkload(rawWorkload)
	if err != nil {
		return s.failAll(stageLoadWorkload, err)
	}
	dev, err := loadDevnet()
	if err != nil {
		return s.failAll(stageBaselineSetup, err)
	}

	var active *activeCase
	defer func() { active.close() }()
	for _, sample := range req.Samples {
		if reason, ok := wl.unsupported[sample.CaseID]; ok {
			row := s.base(sample, statusUnsupported)
			row.Error = &resultError{Stage: stageLoadWorkload, Message: reason}
			if err := s.write(row); err != nil {
				return err
			}
			continue
		}
		rc, ok := wl.ready[sample.CaseID]
		if !ok {
			if err := s.write(s.failed(sample, stageLoadWorkload, errors.New("case_id is absent from workload"))); err != nil {
				return err
			}
			continue
		}
		if active == nil || active.id != sample.CaseID {
			active.close()
			active = openCase(dev, wl, rc)
		}
		var row result
		if active.err != nil {
			row = s.failed(sample, active.stage, active.err)
		} else {
			row = active.run.sample(s, sample)
		}
		if err := s.write(row); err != nil {
			return err
		}
	}
	return out.Close()
}

func openCase(dev *devnet, wl *workload, rc rawCase) *activeCase {
	a := &activeCase{id: rc.ID}
	wc, err := parseCase(rc)
	if err != nil {
		a.stage, a.err = stageLoadWorkload, fmt.Errorf("case %s: %w", rc.ID, err)
		return a
	}
	a.run, a.stage, a.err = newCaseRun(dev, wc, wl.sha)
	return a
}

func validateRequest(req *request) error {
	if req.SchemaVersion != schemaVersion {
		return fmt.Errorf("request schema_version %d is not supported (want %d)", req.SchemaVersion, schemaVersion)
	}
	if req.WorkloadPath == "" || req.SessionID == "" {
		return errors.New("request requires workload_path and session_id")
	}
	if req.Mode != modeDiagnostic && req.Mode != modePerformance {
		return fmt.Errorf("request mode %q is unknown", req.Mode)
	}
	if len(req.Samples) == 0 {
		return errors.New("request must schedule at least one sample")
	}
	seen := make(map[string]struct{}, len(req.Samples))
	for _, sample := range req.Samples {
		if sample.SampleID == "" || sample.CaseID == "" {
			return errors.New("request sample ids and case ids must be non-empty")
		}
		if _, dup := seen[sample.SampleID]; dup {
			return fmt.Errorf("duplicate request sample_id %q", sample.SampleID)
		}
		seen[sample.SampleID] = struct{}{}
		var compatible bool
		switch sample.Phase {
		case phaseDiagnostic:
			compatible = req.Mode == modeDiagnostic
		case phasePilot, phaseWarmup, phaseQualification:
			compatible = req.Mode == modePerformance
		default:
			return fmt.Errorf("request sample %q has unknown phase %q", sample.SampleID, sample.Phase)
		}
		if !compatible {
			return fmt.Errorf("request sample %q phase %q is incompatible with %s mode", sample.SampleID, sample.Phase, req.Mode)
		}
	}
	return nil
}

type sink struct {
	out *os.File
	req *request
}

func (s *sink) write(row result) error {
	line, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("encoding result %q: %w", row.SampleID, err)
	}
	if _, err := s.out.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("writing result %q: %w", row.SampleID, err)
	}
	return nil
}

func (s *sink) failAll(stage string, cause error) error {
	if len(s.req.Samples) == 0 {
		return cause
	}
	for _, sample := range s.req.Samples {
		if err := s.write(s.failed(sample, stage, cause)); err != nil {
			return err
		}
	}
	return s.out.Close()
}

func (s *sink) base(sample requestSample, status string) result {
	return result{
		SchemaVersion:     schemaVersion,
		SessionID:         s.req.SessionID,
		SampleID:          sample.SampleID,
		CaseID:            sample.CaseID,
		Repetition:        sample.Repetition,
		Phase:             sample.Phase,
		Status:            status,
		ExecutionBoundary: executionBoundary,
	}
}

func (s *sink) failed(sample requestSample, stage string, cause error) result {
	row := s.base(sample, statusFailed)
	row.Error = &resultError{Stage: stage, Message: cause.Error()}
	return row
}

func (r *caseRun) withIdentity(row result) result {
	row.BaselineHash = &r.baselineHash
	row.PreparedHash = &r.preparedHash
	declared := r.declaredGas
	row.DeclaredGas = &declared
	return row
}

func (r *caseRun) sample(s *sink, sample requestSample) result {
	diagnostic := s.req.Mode == modeDiagnostic
	var (
		counter *opCounter
		hooks   *tracing.Hooks
	)
	if diagnostic {
		counter = newOpCounter(r.senders, r.wc.watched)
		hooks = counter.hooks()
	}
	obs, err := r.execute(hooks)
	if err != nil {
		return r.withIdentity(s.failed(sample, stageExecute, err))
	}
	var duration *uint64
	if !diagnostic {
		ns := uint64(obs.duration.Nanoseconds())
		duration = &ns
	}
	v, err := r.verify(obs)
	if err != nil {
		row := r.withIdentity(s.failed(sample, stageVerify, err))
		row.ExecutionDurationNS = duration
		if v != nil {
			row.ChargedGas = &v.charged
			row.CommitmentHash = &v.commitment
		}
		return row
	}
	row := r.withIdentity(s.base(sample, statusExecuted))
	row.ExecutionDurationNS = duration
	row.ChargedGas = &v.charged
	row.CommitmentHash = &v.commitment
	row.CorrectnessPassed = true
	if !diagnostic {
		return row
	}
	target, err := counter.target(r.wc.semanticKey)
	if err != nil {
		return failedAfter(row, stageDiagnostic, err)
	}
	row.TargetCount = &target
	row.OpcodeCounts = counter.counts()
	if want := r.wc.expectedTargetCount; want != nil && *want != target {
		return failedAfter(row, stageVerify, fmt.Errorf("requested %d precompile invocations, observed %d", *want, target))
	}
	if !r.imported {
		if err := r.importMeasured(); err != nil {
			return failedAfter(row, stageImport, err)
		}
		r.imported = true
	}
	return row
}

func failedAfter(row result, stage string, cause error) result {
	row.Status = statusFailed
	row.CorrectnessPassed = false
	row.Error = &resultError{Stage: stage, Message: cause.Error()}
	return row
}
