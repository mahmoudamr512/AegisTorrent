use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct MerkleTree {
    nodes: Vec<[u8; 32]>,
    leaf_count: usize,
}

#[derive(Debug, Clone)]
pub struct MerkleProof {
    pub leaf_index: usize,
    pub siblings: Vec<(bool, [u8; 32])>,
}

impl MerkleTree {
    pub fn from_leaves(leaves: &[[u8; 32]]) -> Self {
        assert!(!leaves.is_empty(), "cannot build tree from empty leaves");

        let leaf_count = leaves.len().next_power_of_two();
        let mut nodes = vec![[0u8; 32]; 2 * leaf_count];

        for (i, leaf) in leaves.iter().enumerate() {
            nodes[leaf_count + i] = *leaf;
        }

        for i in leaves.len()..leaf_count {
            nodes[leaf_count + i] = nodes[leaf_count + leaves.len() - 1];
        }

        for i in (1..leaf_count).rev() {
            nodes[i] = hash_pair(&nodes[2 * i], &nodes[2 * i + 1]);
        }

        Self { nodes, leaf_count }
    }

    pub fn root(&self) -> [u8; 32] {
        self.nodes[1]
    }

    pub fn proof(&self, leaf_index: usize) -> MerkleProof {
        let mut siblings = Vec::new();
        let mut idx = self.leaf_count + leaf_index;

        while idx > 1 {
            let sibling = if idx % 2 == 0 { idx + 1 } else { idx - 1 };
            let is_left = idx % 2 != 0;
            siblings.push((is_left, self.nodes[sibling]));
            idx /= 2;
        }

        MerkleProof {
            leaf_index,
            siblings,
        }
    }

    pub fn verify(root: &[u8; 32], leaf: &[u8; 32], proof: &MerkleProof) -> bool {
        let mut current = *leaf;

        for (is_left, sibling) in &proof.siblings {
            current = if *is_left {
                hash_pair(sibling, &current)
            } else {
                hash_pair(&current, sibling)
            };
        }

        current == *root
    }
}

fn hash_pair(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(data: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(data);
        h.finalize().into()
    }

    #[test]
    fn single_leaf_tree() {
        let leaves = vec![hash(b"block0")];
        let tree = MerkleTree::from_leaves(&leaves);
        let proof = tree.proof(0);
        assert!(MerkleTree::verify(&tree.root(), &leaves[0], &proof));
    }

    #[test]
    fn four_leaf_tree() {
        let leaves: Vec<[u8; 32]> = (0..4)
            .map(|i| hash(format!("block{i}").as_bytes()))
            .collect();
        let tree = MerkleTree::from_leaves(&leaves);

        for (i, leaf) in leaves.iter().enumerate() {
            let proof = tree.proof(i);
            assert!(MerkleTree::verify(&tree.root(), leaf, &proof));
        }
    }

    #[test]
    fn tampered_leaf_fails_verification() {
        let leaves: Vec<[u8; 32]> = (0..4)
            .map(|i| hash(format!("block{i}").as_bytes()))
            .collect();
        let tree = MerkleTree::from_leaves(&leaves);
        let proof = tree.proof(0);
        let fake = hash(b"tampered");
        assert!(!MerkleTree::verify(&tree.root(), &fake, &proof));
    }

    #[test]
    fn non_power_of_two_leaves() {
        let leaves: Vec<[u8; 32]> = (0..5)
            .map(|i| hash(format!("block{i}").as_bytes()))
            .collect();
        let tree = MerkleTree::from_leaves(&leaves);
        let proof = tree.proof(2);
        assert!(MerkleTree::verify(&tree.root(), &leaves[2], &proof));
    }
}
