use postcard::ser_flavors::Flavor;

use crate::{LENGTH_PREFIX_LEN, MAX_MESSAGE_BODY_LEN};

// A Postcard storage adapter, not a serializer. The prefix is reserved as
// initialized bytes; the body is appended once with a bound checked before growth.
pub(crate) struct BoundedFrame<'a> {
    pub(crate) bytes: &'a mut Vec<u8>,
    pub(crate) too_large: &'a mut bool,
}

impl Flavor for BoundedFrame<'_> {
    type Output = ();

    fn try_push(&mut self, byte: u8) -> postcard::Result<()> {
        self.try_extend(&[byte])
    }

    fn try_extend(&mut self, bytes: &[u8]) -> postcard::Result<()> {
        // The caller initializes the prefix and only this flavor appends bytes.
        let body_len = self.bytes.len() - LENGTH_PREFIX_LEN;
        if bytes.len() > MAX_MESSAGE_BODY_LEN - body_len {
            *self.too_large = true;
            return Err(postcard::Error::SerializeBufferFull);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn finalize(self) -> postcard::Result<()> {
        Ok(())
    }
}
