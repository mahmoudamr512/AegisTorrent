pub struct SlidingWindow {
    values: Vec<f64>,
    capacity: usize,
    cursor: usize,
    count: usize,
}

impl SlidingWindow {
    pub fn new(capacity: usize) -> Self {
        Self {
            values: vec![0.0; capacity],
            capacity,
            cursor: 0,
            count: 0,
        }
    }

    pub fn push(&mut self, value: f64) {
        self.values[self.cursor] = value;
        self.cursor = (self.cursor + 1) % self.capacity;
        if self.count < self.capacity {
            self.count += 1;
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let sum: f64 = self.values[..self.count].iter().sum();
        sum / self.count as f64
    }

    pub fn median(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let mut sorted: Vec<f64> = self.values[..self.count].to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if self.count % 2 == 0 {
            (sorted[self.count / 2 - 1] + sorted[self.count / 2]) / 2.0
        } else {
            sorted[self.count / 2]
        }
    }

    pub fn percentile(&self, p: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let mut sorted: Vec<f64> = self.values[..self.count].to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p / 100.0) * (self.count - 1) as f64).round() as usize;
        sorted[idx.min(self.count - 1)]
    }

    pub fn sum(&self) -> f64 {
        self.values[..self.count].iter().sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_window() {
        let w = SlidingWindow::new(10);
        assert!(w.is_empty());
        assert_eq!(w.mean(), 0.0);
        assert_eq!(w.median(), 0.0);
    }

    #[test]
    fn push_and_mean() {
        let mut w = SlidingWindow::new(5);
        w.push(10.0);
        w.push(20.0);
        w.push(30.0);
        assert_eq!(w.len(), 3);
        assert!((w.mean() - 20.0).abs() < 1e-9);
    }

    #[test]
    fn median_odd() {
        let mut w = SlidingWindow::new(5);
        w.push(3.0);
        w.push(1.0);
        w.push(2.0);
        assert!((w.median() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn median_even() {
        let mut w = SlidingWindow::new(5);
        w.push(1.0);
        w.push(2.0);
        w.push(3.0);
        w.push(4.0);
        assert!((w.median() - 2.5).abs() < 1e-9);
    }

    #[test]
    fn wraps_around() {
        let mut w = SlidingWindow::new(3);
        w.push(1.0);
        w.push(2.0);
        w.push(3.0);
        w.push(100.0);
        assert_eq!(w.len(), 3);
        assert!((w.mean() - 35.0).abs() < 1e-9);
    }

    #[test]
    fn percentile_values() {
        let mut w = SlidingWindow::new(100);
        for i in 1..=100 {
            w.push(i as f64);
        }
        assert!((w.percentile(50.0) - 50.0).abs() < 1.5);
        assert!((w.percentile(90.0) - 90.0).abs() < 1.5);
    }
}
