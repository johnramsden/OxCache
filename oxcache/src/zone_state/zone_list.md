# Testing

`state_tracker` keeps track of the state of all zones. The intention is
to make sure that incorrect operations can\'t happen and to provide
better debugging output.

It keeps track of all state transitions and makes sure that the state
transitions are correct. E.g. it will error when there are more active
zones than the max.

This *should* probably be extracted into its own thing with its own DSL
but it should work as is. Chunk is a little tricky and hasn\'t been
implemented yet.
