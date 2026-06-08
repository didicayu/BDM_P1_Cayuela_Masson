# CyberSecIntel P2 Final Delivery

This folder contains the final P2 report source and build target.

```bash
make -C docs/p2_final_delivery rebuild
```

From the repository root, `scripts/build_delivery.sh` rebuilds this PDF, refreshes
the root copy, creates the final ZIP from an explicit allowlist, and runs `unzip -t`.

The final report consolidates the P2.1 architecture and P2.2 implementation into
the required final P2 sections: instructions, architecture, Trusted Zone,
Exploitation Zone, Data Consumption, and Governance.

The current final version also documents the closing improvements for Spark
execution paths, Spark Structured Streaming warm enrichment, sklearn Isolation
Forest artifacts, warm stream aggregates, and Grafana provisioning.
