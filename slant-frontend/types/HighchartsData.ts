import HighchartsDataSeries from "./HighchartsDataSeries";

interface HighchartsData {
    x: number[];
    series: HighchartsDataSeries[];
    mode: string;
}

export default HighchartsData;