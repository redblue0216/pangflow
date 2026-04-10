# pangflow Templates

This directory contains template files for pangflow.

## Workflow Templates

Workflow templates can be generated using the CLI:

```bash
pangflowctl create <name> --type trigger --package PangTS
pangflowctl create <name> --type scheduled --package PangFT
```

## Template Structure

- `workflow-trigger.toml` - Template for trigger-based workflows (training)
- `workflow-scheduled.toml` - Template for scheduled workflows (inference)
