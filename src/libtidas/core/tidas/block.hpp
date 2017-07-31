/*
  TImestream DAta Storage (TIDAS)
  Copyright (c) 2014-2017, all rights reserved.  Use of this source code 
  is governed by a BSD-style license that can be found in the top-level
  LICENSE file.
*/

#ifndef TIDAS_BLOCK_HPP
#define TIDAS_BLOCK_HPP


namespace tidas {

    // forward declare copy functions

    class block;

    void data_copy ( intervals const & in, intervals & out );
    void data_copy ( group const & in, group & out );
    void data_copy ( block const & in, block & out );


    /// A block represents a logical grouping of data.  A block can have zero
    /// or more groups, intervals and sub-blocks.  Child objects are added
    /// with a "name" associated to them.  These names are used when performing
    /// selection / query operations.  
    /// Some public methods are only used internally and are not needed for
    /// normal use of the object.  These are labelled "internal".

    class block {

        public :

            /// Default constructor.
            block ();

            /// Destructor
            ~block ();
            
            /// Assignment operator.
            block & operator= ( block const & other );

            /// Copy constructor.
            block ( block const & other );

            /// (**Internal**) Load a block from disk with the specified 
            /// selection filter.
            block ( backend_path const & loc, std::string const & filter = "" );
            
            /// (**Internal**) Create a copy of a block, with optional selection and new location.
            block ( block const & other, std::string const & filter, backend_path const & loc );

            // metadata ops

            /// (**Internal**) Change the location of the block.
            void relocate ( backend_path const & loc );

            /// (**Internal**) Recursively read the metadata for the block and its
            /// children from disk.  Optionally apply a selection filter.
            void sync ( std::string const & filter = "" );

            /// (**Internal**) Recursively write metadata for the block and its 
            /// children.
            void flush () const;

            /// (**Internal**) Copy with optional selection and relocation.
            void copy ( block const & other, std::string const & filter, backend_path const & loc );

            /// (**Internal**) Create a link at the specified location.
            void link ( link_type const & type, std::string const & path ) const;

            /// (**Internal**) Delete the on-disk data and metadata associated 
            /// with this object.  In-memory metadata is not modified.
            void wipe () const;

            /// (**Internal**) The current location.
            backend_path location () const;

            /// Return the filesystem path of the auxilliary directory for 
            /// this block.
            std::string aux_dir () const;

            // data ops

            /// Returns the extrema of timestamps of all groups in this block.
            /// This is not recursive (does not traverse sub-blocks).
            void range ( time_type & start, time_type & stop ) const;

            /// Removes all groups, intervals, and sub-blocks from this block.
            void clear();

            /// Add a group to this block using the specified name.  Returns
            /// a refererence to the newly added group.
            group & group_add ( std::string const & name, group const & grp );

            /// Get a (non-const) reference to the group with the specified
            /// name.
            group & group_get ( std::string const & name );

            /// Get a (non-const) reference to the group with the specified
            /// name.
            group const & group_get ( std::string const & name ) const;
            
            /// Delete the specified group.
            void group_del ( std::string const & name );

            /// Return a list of the names of all groups.
            std::vector < std::string > all_groups () const;
            
            /// Remove all groups from this block.
            void clear_groups();

            /// Add an intervals object to this block using the specified 
            /// name.  Returns a refererence to the newly added intervals.
            intervals & intervals_add ( std::string const & name, intervals const & intr );
            
            /// Get a (non-const) reference to the intervals object with 
            /// the specified name.
            intervals & intervals_get ( std::string const & name );

            /// Get a (const) reference to the intervals object with 
            /// the specified name.
            intervals const & intervals_get ( std::string const & name ) const;
            
            /// Delete the specified intervals object.
            void intervals_del ( std::string const & name );
            
            /// Return a list of the names of all intervals.
            std::vector < std::string > all_intervals () const;
            
            /// Remove all intervals objects from this block.
            void clear_intervals();

            /// Add a sub-block to this block using the specified name.
            /// Returns a reference to the newly added block.
            block & block_add ( std::string const & name, block const & blk );
            
            /// Get a (non-const) reference to the sub-block with the
            /// specified name.
            block & block_get ( std::string const & name );

            /// Get a (const) reference to the sub-block with the
            /// specified name.
            block const & block_get ( std::string const & name ) const;
            
            /// Remove the specified sub-block.
            void block_del ( std::string const & name );
            
            /// Return a list of the names of all sub-blocks.
            std::vector < std::string > all_blocks () const;
            
            /// Remove all sub-blocks from this block.
            void clear_blocks();

            /// Recursively copy the current block and all descendents,
            /// applying the specified matching pattern.
            block select ( std::string const & filter = "" ) const;

            /// (**Internal**) Print metadat info for this block.  Mainly
            /// used for debugging.
            void info ( std::string name, bool recurse, size_t indent );


            // non-const version

            /// Pass over this block and all descendents, calling a functor
            /// on each one.  The specified class should provide the
            /// operator() method.  Blocks may be modified by this version.
            template < class P >
            void exec ( P & op, exec_order order ) {

                if ( order == exec_order::depth_last ) {
                    op ( *this );
                }

                if ( block_data_.size() == 0 ) {

                    // this is a leaf
                    if ( order == exec_order::leaf ) {
                        op ( *this );
                    }
                
                } else {
                    // operate on sub blocks

                    for ( std::map < std::string, block > :: const_iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
                        it->second.exec ( op, order );
                    }

                }

                if ( order == exec_order::depth_first ) {
                    op ( *this );
                }

                return;
            }


            // const version
            
            /// Pass over this block and all descendents, calling a functor
            /// on each one.  The specified class should provide the
            /// operator() method.  Blocks are treated as const in this version.
            template < class P >
            void exec ( P & op, exec_order order ) const {

                if ( order == exec_order::depth_last ) {
                    op ( *this );
                }

                if ( block_data_.size() == 0 ) {

                    // this is a leaf
                    if ( order == exec_order::leaf ) {
                        op ( *this );
                    }
                
                } else {
                    // operate on sub blocks

                    for ( std::map < std::string, block > :: const_iterator it = block_data_.begin(); it != block_data_.end(); ++it ) {
                        it->second.exec ( op, order );
                    }

                }

                if ( order == exec_order::depth_first ) {
                    op ( *this );
                }

                return;
            }

            template < class Archive >
            void save ( Archive & ar ) const {
                ar ( CEREAL_NVP( loc_ ) );
                ar ( CEREAL_NVP( intervals_data_ ) );
                ar ( CEREAL_NVP( group_data_ ) );
                ar ( CEREAL_NVP( block_data_ ) );
            }

            template < class Archive >
            void load ( Archive & ar ) {
                ar ( CEREAL_NVP( loc_ ) );
                ar ( CEREAL_NVP( intervals_data_ ) );
                ar ( CEREAL_NVP( group_data_ ) );
                ar ( CEREAL_NVP( block_data_ ) );
            }

        private :

            backend_path group_loc ( backend_path const & loc, std::string const & name ) const;
            
            backend_path intervals_loc ( backend_path const & loc, std::string const & name ) const;
            
            backend_path block_loc ( backend_path const & loc, std::string const & name ) const;

            std::map < std::string, block > block_data_;
            std::map < std::string, group > group_data_;
            std::map < std::string, intervals > intervals_data_;

            backend_path loc_;

    };


    class block_link_operator {

        public :

            void operator() ( block const & blk );

            link_type type;
            std::string path;
            std::string start;

    };


    class block_wipe_operator {

        public :

            void operator() ( block const & blk );

    };


}

#endif
