-record(label, {operation :: remote_read | update | remote_reply,
                bkey,
                timestamp :: non_neg_integer(),
                node,
                sender :: non_neg_integer(),
                payload}).

-define(PROPAGATION_MODE, naive_erlang).
