import type { BackendApi } from "../backend";

export type PanelProps = {
  api: BackendApi;
  setMessage: (message: string) => void;
};
