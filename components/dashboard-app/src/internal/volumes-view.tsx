"use client";

import { useRouter } from "next/navigation";
import { useState, useTransition } from "react";
import {
  PixelBadge,
  PixelButton,
  PixelCard,
  PixelInput,
  PixelSelect,
} from "@sandbox0/ui";

import type { DashboardVolumeSummary } from "./types";

export interface DashboardVolumesViewProps {
  volumes: DashboardVolumeSummary[];
  brandName?: string;
}

interface CreateVolumeFormData {
  cacheSize: string;
  bufferSize: string;
  accessMode: string;
  writeback: boolean;
}

function formatDate(isoString: string): string {
  return new Date(isoString).toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function AccessModeBadge({ mode }: { mode?: string }) {
  const variants: Record<string, "default" | "accent" | "success"> = {
    RWO: "default",
    ROX: "success",
    RWX: "accent",
  };
  const label = mode ?? "RWO";
  return <PixelBadge variant={variants[label] ?? "default"}>{label}</PixelBadge>;
}

interface CreateVolumeDialogProps {
  onClose: () => void;
  onCreated: () => void;
}

function CreateVolumeDialog({ onClose, onCreated }: CreateVolumeDialogProps) {
  const [form, setForm] = useState<CreateVolumeFormData>({
    cacheSize: "512MiB",
    bufferSize: "100MiB",
    accessMode: "RWO",
    writeback: true,
  });
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    startTransition(async () => {
      try {
        const res = await fetch("/api/volumes", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            cache_size: form.cacheSize,
            buffer_size: form.bufferSize,
            access_mode: form.accessMode,
            writeback: form.writeback,
          }),
        });

        if (!res.ok) {
          const data = (await res.json().catch(() => null)) as {
            error?: { message?: string };
          } | null;
          setError(data?.error?.message ?? `Request failed: ${res.status}`);
          return;
        }

        onCreated();
        onClose();
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      }
    });
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-background/80 backdrop-blur-sm">
      <PixelCard header="Create Volume" className="w-full max-w-md">
        <form onSubmit={handleSubmit} className="space-y-4">
          <PixelInput
            label="Cache Size"
            value={form.cacheSize}
            onChange={(e) => setForm((f) => ({ ...f, cacheSize: e.target.value }))}
            placeholder="e.g. 512MiB"
          />
          <PixelInput
            label="Buffer Size"
            value={form.bufferSize}
            onChange={(e) =>
              setForm((f) => ({ ...f, bufferSize: e.target.value }))
            }
            placeholder="e.g. 100MiB"
          />
          <div>
            <p className="mb-1 text-xs text-muted">Access Mode</p>
            <PixelSelect
              ariaLabel="Access Mode"
              value={form.accessMode}
              onValueChange={(value) =>
                setForm((f) => ({ ...f, accessMode: value }))
              }
              options={[
                { value: "RWO", label: "RWO — Read-Write Once" },
                { value: "ROX", label: "ROX — Read-Only Many" },
                { value: "RWX", label: "RWX — Read-Write Many" },
              ]}
            />
          </div>
          <label className="flex cursor-pointer items-center gap-2 text-sm">
            <input
              type="checkbox"
              checked={form.writeback}
              onChange={(e) =>
                setForm((f) => ({ ...f, writeback: e.target.checked }))
              }
              className="accent-accent"
            />
            Enable writeback
          </label>

          {error && (
            <p className="font-mono text-xs text-red-500">{error}</p>
          )}

          <div className="flex gap-3 pt-2">
            <PixelButton variant="primary" type="submit" disabled={isPending}>
              {isPending ? "Creating..." : "Create Volume"}
            </PixelButton>
            <PixelButton
              variant="secondary"
              type="button"
              onClick={onClose}
              disabled={isPending}
            >
              Cancel
            </PixelButton>
          </div>
        </form>
      </PixelCard>
    </div>
  );
}

interface VolumeRowProps {
  volume: DashboardVolumeSummary;
  onDelete: (id: string) => Promise<void>;
  onFork: (id: string) => Promise<void>;
}

function VolumeRow({ volume, onDelete, onFork }: VolumeRowProps) {
  const [isDeleting, startDeleteTransition] = useTransition();
  const [isForking, startForkTransition] = useTransition();

  return (
    <PixelCard scale="sm" interactive>
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <div className="mb-1 flex items-center gap-2">
            <p className="truncate font-mono text-sm">{volume.id}</p>
            <AccessModeBadge mode={volume.accessMode} />
            {volume.sourceVolumeID && (
              <PixelBadge variant="warning">Fork</PixelBadge>
            )}
          </div>
          <div className="flex items-center gap-4 text-xs text-muted">
            <span>Cache: {volume.cacheSize}</span>
            <span>Buffer: {volume.bufferSize}</span>
            {volume.writeback !== undefined && (
              <span>Writeback: {volume.writeback ? "on" : "off"}</span>
            )}
          </div>
          <p className="mt-1 text-xs text-muted">
            Created {formatDate(volume.createdAt)}
          </p>
        </div>
        <div className="flex shrink-0 gap-2">
          <PixelButton
            variant="secondary"
            scale="sm"
            disabled={isForking || isDeleting}
            onClick={() => startForkTransition(() => onFork(volume.id))}
          >
            {isForking ? "Forking..." : "Fork"}
          </PixelButton>
          <PixelButton
            variant="secondary"
            scale="sm"
            disabled={isDeleting || isForking}
            onClick={() => startDeleteTransition(() => onDelete(volume.id))}
          >
            {isDeleting ? "Deleting..." : "Delete"}
          </PixelButton>
        </div>
      </div>
    </PixelCard>
  );
}

export function DashboardVolumesView({
  volumes,
  brandName = "SANDBOX0",
}: DashboardVolumesViewProps) {
  const router = useRouter();
  const [showCreate, setShowCreate] = useState(false);
  const [pageError, setPageError] = useState<string | null>(null);

  function refresh() {
    router.refresh();
  }

  async function handleDelete(id: string) {
    setPageError(null);
    const res = await fetch(`/api/volumes/${id}`, { method: "DELETE" });
    if (!res.ok) {
      const data = (await res.json().catch(() => null)) as {
        error?: { message?: string };
      } | null;
      setPageError(data?.error?.message ?? `Delete failed: ${res.status}`);
      return;
    }
    refresh();
  }

  async function handleFork(id: string) {
    setPageError(null);
    const res = await fetch(`/api/volumes/${id}/fork`, { method: "POST" });
    if (!res.ok) {
      const data = (await res.json().catch(() => null)) as {
        error?: { message?: string };
      } | null;
      setPageError(data?.error?.message ?? `Fork failed: ${res.status}`);
      return;
    }
    refresh();
  }

  return (
    <>
      <header className="flex items-center justify-between border-b border-foreground/10 p-4">
        <div className="flex items-center gap-3">
          <h1 className="font-pixel text-sm">{brandName}</h1>
        </div>
        <nav className="flex gap-4">
          <a href="/">
            <PixelButton variant="secondary" scale="sm">
              Overview
            </PixelButton>
          </a>
          <a href="/volumes">
            <PixelButton variant="primary" scale="sm">
              Volumes
            </PixelButton>
          </a>
        </nav>
        <div className="flex gap-3">
          <form action="/api/auth/logout" method="post">
            <PixelButton variant="secondary" scale="sm" type="submit">
              Logout
            </PixelButton>
          </form>
        </div>
      </header>

      <main className="flex-1 p-6">
        <div className="mb-8 flex items-end justify-between">
          <div>
            <h2 className="mb-1 font-pixel text-lg">Volumes</h2>
            <p className="text-sm text-muted">
              {volumes.length} persistent volume{volumes.length !== 1 ? "s" : ""}
            </p>
          </div>
          <PixelButton
            variant="primary"
            scale="sm"
            onClick={() => setShowCreate(true)}
          >
            + New Volume
          </PixelButton>
        </div>

        {pageError && (
          <div className="mb-4 border border-red-500/30 bg-red-500/10 p-3">
            <p className="font-mono text-xs text-red-400">{pageError}</p>
          </div>
        )}

        {volumes.length > 0 ? (
          <div className="space-y-3">
            {volumes.map((volume) => (
              <VolumeRow
                key={volume.id}
                volume={volume}
                onDelete={handleDelete}
                onFork={handleFork}
              />
            ))}
          </div>
        ) : (
          <PixelCard header="No Volumes">
            <p className="mb-4 text-sm text-muted">
              Volumes provide persistent storage for your sandboxes. Create a
              volume to get started.
            </p>
            <PixelButton
              variant="primary"
              scale="sm"
              onClick={() => setShowCreate(true)}
            >
              + Create First Volume
            </PixelButton>
          </PixelCard>
        )}
      </main>

      <footer className="border-t border-foreground/10 p-4 text-center text-xs text-muted">
        Sandbox0 Dashboard
      </footer>

      {showCreate && (
        <CreateVolumeDialog
          onClose={() => setShowCreate(false)}
          onCreated={refresh}
        />
      )}
    </>
  );
}
