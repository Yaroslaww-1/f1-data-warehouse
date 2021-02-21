import { LoadCircuitDim } from './load-circuit-dim/load-circuit-dim';

export class LoadDataWarehouse {
  static async load() {
    await LoadCircuitDim.load();
  }
}