use rand::seq::SliceRandom;
use std::collections::HashMap;

pub type PeerId = [u8; 20];

pub struct Scheduler {
    piece_count: u32,
    our_bitfield: Vec<bool>,
    peer_bitfields: HashMap<PeerId, Vec<bool>>,
    in_flight: HashMap<u32, PeerId>,
    piece_rarity: Vec<u32>,
    endgame: bool,
}

#[derive(Debug, Clone)]
pub struct PieceAssignment {
    pub piece_index: u32,
    pub peer: PeerId,
}

impl Scheduler {
    pub fn new(piece_count: u32) -> Self {
        Self {
            piece_count,
            our_bitfield: vec![false; piece_count as usize],
            peer_bitfields: HashMap::new(),
            in_flight: HashMap::new(),
            piece_rarity: vec![0; piece_count as usize],
            endgame: false,
        }
    }

    pub fn register_peer(&mut self, peer: PeerId, bitfield: &[bool]) {
        for (i, &has) in bitfield.iter().enumerate() {
            if has {
                self.piece_rarity[i] += 1;
            }
        }
        self.peer_bitfields.insert(peer, bitfield.to_vec());
    }

    pub fn unregister_peer(&mut self, peer: &PeerId) {
        if let Some(bf) = self.peer_bitfields.remove(peer) {
            for (i, &has) in bf.iter().enumerate() {
                if has {
                    self.piece_rarity[i] = self.piece_rarity[i].saturating_sub(1);
                }
            }
        }
        self.in_flight.retain(|_, p| p != peer);
    }

    pub fn peer_has_piece(&mut self, peer: &PeerId, index: u32) {
        if let Some(bf) = self.peer_bitfields.get_mut(peer) {
            let i = index as usize;
            if i < bf.len() && !bf[i] {
                bf[i] = true;
                self.piece_rarity[i] += 1;
            }
        }
    }

    pub fn pick_piece(&mut self, peer: &PeerId) -> Option<PieceAssignment> {
        let peer_bf = self.peer_bitfields.get(peer)?;

        let mut candidates: Vec<u32> = (0..self.piece_count)
            .filter(|&i| {
                let idx = i as usize;
                !self.our_bitfield[idx]
                    && peer_bf[idx]
                    && (self.endgame || !self.in_flight.contains_key(&i))
            })
            .collect();

        if candidates.is_empty() {
            return None;
        }

        candidates.sort_by_key(|&i| self.piece_rarity[i as usize]);
        let min_rarity = self.piece_rarity[candidates[0] as usize];
        let rarest: Vec<u32> = candidates
            .into_iter()
            .take_while(|&i| self.piece_rarity[i as usize] == min_rarity)
            .collect();

        let mut rng = rand::thread_rng();
        let &chosen = rarest.choose(&mut rng).unwrap();

        if !self.endgame {
            self.in_flight.insert(chosen, *peer);
            self.check_endgame();
        }

        Some(PieceAssignment {
            piece_index: chosen,
            peer: *peer,
        })
    }

    pub fn piece_completed(&mut self, index: u32) {
        let i = index as usize;
        self.our_bitfield[i] = true;
        self.in_flight.remove(&index);
        self.check_endgame();
    }

    pub fn piece_failed(&mut self, index: u32) {
        self.in_flight.remove(&index);
    }

    pub fn is_complete(&self) -> bool {
        self.our_bitfield.iter().all(|&b| b)
    }

    pub fn is_endgame(&self) -> bool {
        self.endgame
    }

    pub fn missing_pieces(&self) -> Vec<u32> {
        (0..self.piece_count)
            .filter(|&i| !self.our_bitfield[i as usize])
            .collect()
    }

    pub fn endgame_requests(&self) -> Vec<PieceAssignment> {
        if !self.endgame {
            return vec![];
        }
        let mut assignments = vec![];
        for piece in self.missing_pieces() {
            for (peer, bf) in &self.peer_bitfields {
                if bf[piece as usize] {
                    assignments.push(PieceAssignment {
                        piece_index: piece,
                        peer: *peer,
                    });
                }
            }
        }
        assignments
    }

    pub fn in_flight_for_piece(&self, index: u32) -> Option<&PeerId> {
        self.in_flight.get(&index)
    }

    pub fn peers_with_piece(&self, index: u32) -> Vec<PeerId> {
        self.peer_bitfields
            .iter()
            .filter_map(|(peer, bf)| {
                if bf.get(index as usize).copied().unwrap_or(false) {
                    Some(*peer)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn piece_rarity(&self, index: u32) -> u32 {
        self.piece_rarity.get(index as usize).copied().unwrap_or(0)
    }

    fn check_endgame(&mut self) {
        if self.endgame {
            return;
        }
        let missing: Vec<u32> = self.missing_pieces();
        if !missing.is_empty() && missing.iter().all(|i| self.in_flight.contains_key(i)) {
            self.endgame = true;
        }
    }
}

pub fn parse_bitfield_bytes(bytes: &[u8], piece_count: u32) -> Vec<bool> {
    let mut result = Vec::with_capacity(piece_count as usize);
    for i in 0..piece_count as usize {
        let byte_idx = i / 8;
        let bit_idx = 7 - (i % 8);
        let has = if byte_idx < bytes.len() {
            (bytes[byte_idx] >> bit_idx) & 1 == 1
        } else {
            false
        };
        result.push(has);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; 20];
        id[0] = n;
        id
    }

    #[test]
    fn picks_rarest_piece() {
        let mut sched = Scheduler::new(4);
        sched.register_peer(peer(1), &[true, true, true, true]);
        sched.register_peer(peer(2), &[true, true, false, false]);

        let pick = sched.pick_piece(&peer(1)).unwrap();
        assert!(pick.piece_index == 2 || pick.piece_index == 3);
    }

    #[test]
    fn skips_in_flight_pieces() {
        let mut sched = Scheduler::new(3);
        sched.register_peer(peer(1), &[true, true, true]);
        sched.register_peer(peer(2), &[true, true, true]);

        let first = sched.pick_piece(&peer(1)).unwrap();
        let second = sched.pick_piece(&peer(2)).unwrap();
        assert_ne!(first.piece_index, second.piece_index);
    }

    #[test]
    fn returns_none_when_peer_has_nothing_we_need() {
        let mut sched = Scheduler::new(2);
        sched.register_peer(peer(1), &[true, true]);
        sched.piece_completed(0);
        sched.piece_completed(1);
        assert!(sched.pick_piece(&peer(1)).is_none());
    }

    #[test]
    fn piece_completed_marks_done() {
        let mut sched = Scheduler::new(2);
        sched.register_peer(peer(1), &[true, true]);
        sched.piece_completed(0);
        sched.piece_completed(1);
        assert!(sched.is_complete());
    }

    #[test]
    fn endgame_triggers_when_all_missing_are_in_flight() {
        let mut sched = Scheduler::new(3);
        sched.register_peer(peer(1), &[true, true, true]);
        sched.register_peer(peer(2), &[true, true, true]);

        sched.piece_completed(0);
        let _ = sched.pick_piece(&peer(1));
        let _ = sched.pick_piece(&peer(2));

        assert!(sched.is_endgame());
    }

    #[test]
    fn endgame_allows_duplicate_requests() {
        let mut sched = Scheduler::new(2);
        sched.register_peer(peer(1), &[true, true]);
        sched.register_peer(peer(2), &[true, true]);

        sched.piece_completed(0);
        let _ = sched.pick_piece(&peer(1));

        assert!(sched.is_endgame());

        let pick = sched.pick_piece(&peer(2));
        assert!(pick.is_some());
        assert_eq!(pick.unwrap().piece_index, 1);
    }

    #[test]
    fn endgame_requests_returns_all_peers_for_missing() {
        let mut sched = Scheduler::new(3);
        sched.register_peer(peer(1), &[true, true, true]);
        sched.register_peer(peer(2), &[true, false, true]);

        sched.piece_completed(0);
        let _ = sched.pick_piece(&peer(1));
        let _ = sched.pick_piece(&peer(2));

        assert!(sched.is_endgame());
        let reqs = sched.endgame_requests();
        assert!(!reqs.is_empty());
    }

    #[test]
    fn unregister_peer_updates_rarity() {
        let mut sched = Scheduler::new(2);
        sched.register_peer(peer(1), &[true, true]);
        sched.register_peer(peer(2), &[true, false]);

        assert_eq!(sched.piece_rarity[0], 2);
        assert_eq!(sched.piece_rarity[1], 1);

        sched.unregister_peer(&peer(2));
        assert_eq!(sched.piece_rarity[0], 1);
        assert_eq!(sched.piece_rarity[1], 1);
    }

    #[test]
    fn peer_has_piece_updates_state() {
        let mut sched = Scheduler::new(3);
        sched.register_peer(peer(1), &[true, false, false]);

        assert_eq!(sched.piece_rarity[1], 0);
        sched.peer_has_piece(&peer(1), 1);
        assert_eq!(sched.piece_rarity[1], 1);
    }

    #[test]
    fn parse_bitfield_bytes_works() {
        let bytes = vec![0b1110_0000];
        let bf = parse_bitfield_bytes(&bytes, 3);
        assert_eq!(bf, vec![true, true, true]);

        let bytes = vec![0b1010_1010];
        let bf = parse_bitfield_bytes(&bytes, 8);
        assert_eq!(bf, vec![true, false, true, false, true, false, true, false]);
    }

    #[test]
    fn piece_failed_frees_slot() {
        let mut sched = Scheduler::new(2);
        sched.register_peer(peer(1), &[true, true]);
        let pick = sched.pick_piece(&peer(1)).unwrap();
        let idx = pick.piece_index;

        sched.piece_failed(idx);
        assert!(sched.in_flight_for_piece(idx).is_none());
    }
}
