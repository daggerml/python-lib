declare module "*.png" {
  const source: string;
  export default source;
}

declare module "plotly.js-dist-min" {
  const Plotly: {
    newPlot: (target: HTMLElement, data: unknown[], layout?: object, config?: object) => Promise<void>;
    purge: (target: HTMLElement) => void;
  };
  export default Plotly;
}
